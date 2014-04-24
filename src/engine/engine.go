package engine

import (
	"common"
	"fmt"
	"parser"
	"protocol"
	"strconv"
	"strings"
	"time"

	log "code.google.com/p/log4go"
)

var (
	TRUE = true
)

type QueryEngine struct {
	query          *parser.SelectQuery
	fields         []string
	where          *parser.WhereCondition
	responseChan   chan *protocol.Response
	limiter        *Limiter
	seriesToPoints map[string]*protocol.Series
	yield          func(*protocol.Series) error

	// variables for aggregate queries
	isAggregateQuery bool
	aggregators      []Aggregator
	duration         *time.Duration
	buckets          map[string]int64
	pointsRange      map[string]*PointRange
	aggregateYield   func(*protocol.Series) error
	explain          bool
	trie             *Trie
	lastTimestamp    int64
	started          bool
	elems            []*parser.Value
	fillWithZero     bool

	// query statistics
	runStartTime  float64
	runEndTime    float64
	pointsRead    int64
	pointsWritten int64
	shardId       int
	shardLocal    bool
}

var (
	endStreamResponse    = protocol.Response_END_STREAM
	explainQueryResponse = protocol.Response_EXPLAIN_QUERY
)

const (
	POINT_BATCH_SIZE = 64
)

// distribute query and possibly do the merge/join before yielding the points
func (self *QueryEngine) distributeQuery(query *parser.SelectQuery, yield func(*protocol.Series) error) error {
	// see if this is a merge query
	fromClause := query.GetFromClause()
	if fromClause.Type == parser.FromClauseMerge {
		yield = getMergeYield(fromClause.Names[0].Name.Name, fromClause.Names[1].Name.Name, query.Ascending, yield)
	}

	if fromClause.Type == parser.FromClauseInnerJoin {
		yield = getJoinYield(query, yield)
	}

	self.yield = yield
	return nil
}

func NewQueryEngine(query *parser.SelectQuery, responseChan chan *protocol.Response) (*QueryEngine, error) {
	limit := query.Limit

	queryEngine := &QueryEngine{
		query:          query,
		where:          query.GetWhereCondition(),
		limiter:        NewLimiter(limit),
		responseChan:   responseChan,
		seriesToPoints: make(map[string]*protocol.Series),
		// stats stuff
		explain:       query.IsExplainQuery(),
		runStartTime:  0,
		runEndTime:    0,
		pointsRead:    0,
		pointsWritten: 0,
		shardId:       0,
		buckets:       map[string]int64{},
		shardLocal:    false, //that really doesn't matter if it is not EXPLAIN query
		started:       false,
	}

	if queryEngine.explain {
		queryEngine.runStartTime = float64(time.Now().UnixNano()) / float64(time.Millisecond)
	}

	yield := func(series *protocol.Series) error {
		var response *protocol.Response

		queryEngine.limiter.calculateLimitAndSlicePoints(series)
		if len(series.Points) == 0 {
			return nil
		}
		if queryEngine.explain {
			//TODO: We may not have to send points, just count them
			queryEngine.pointsWritten += int64(len(series.Points))
		}
		response = &protocol.Response{Type: &queryResponse, Series: series}
		responseChan <- response
		return nil
	}

	var err error
	if query.HasAggregates() {
		err = queryEngine.executeCountQueryWithGroupBy(query, yield)
	} else if containsArithmeticOperators(query) {
		err = queryEngine.executeArithmeticQuery(query, yield)
	} else {
		err = queryEngine.distributeQuery(query, yield)
	}

	if err != nil {
		return nil, err
	}
	return queryEngine, nil
}

// Shard will call this method for EXPLAIN query
func (self *QueryEngine) SetShardInfo(shardId int, shardLocal bool) {
	self.shardId = shardId
	self.shardLocal = shardLocal
}

// Returns false if the query should be stopped (either because of limit or error)
func (self *QueryEngine) YieldPoint(seriesName *string, fieldNames []string, point *protocol.Point) (shouldContinue bool) {
	shouldContinue = true
	series := self.seriesToPoints[*seriesName]
	if series == nil {
		series = &protocol.Series{Name: protocol.String(*seriesName), Fields: fieldNames, Points: make([]*protocol.Point, 0, POINT_BATCH_SIZE)}
		self.seriesToPoints[*seriesName] = series
	} else if len(series.Points) >= POINT_BATCH_SIZE {
		shouldContinue = self.yieldSeriesData(series)
		series = &protocol.Series{Name: protocol.String(*seriesName), Fields: fieldNames, Points: make([]*protocol.Point, 0, POINT_BATCH_SIZE)}
		self.seriesToPoints[*seriesName] = series
	}
	series.Points = append(series.Points, point)

	if self.explain {
		self.pointsRead++
	}

	return shouldContinue
}

func (self *QueryEngine) YieldSeries(seriesIncoming *protocol.Series) (shouldContinue bool) {
	if self.explain {
		self.pointsRead += int64(len(seriesIncoming.Points))
	}
	seriesName := seriesIncoming.GetName()
	self.seriesToPoints[seriesName] = &protocol.Series{Name: &seriesName, Fields: seriesIncoming.Fields}
	return self.yieldSeriesData(seriesIncoming) && !self.limiter.hitLimit(seriesIncoming.GetName())
}

func (self *QueryEngine) yieldSeriesData(series *protocol.Series) bool {
	err := self.yield(series)
	if err != nil {
		log.Error(err)
		return false
	}
	return true
}

func (self *QueryEngine) Close() {
	for _, series := range self.seriesToPoints {
		if len(series.Points) == 0 {
			continue
		}
		self.yieldSeriesData(series)
	}

	var err error
	for _, series := range self.seriesToPoints {
		s := &protocol.Series{
			Name:   series.Name,
			Fields: series.Fields,
		}
		err = self.yield(s)
		if err != nil {
			break
		}
	}

	if self.isAggregateQuery {
		self.runAggregates()
	}

	if self.explain {
		self.runEndTime = float64(time.Now().UnixNano()) / float64(time.Millisecond)
		log.Debug("QueryEngine: %.3f R:%d W:%d", self.runEndTime-self.runStartTime, self.pointsRead, self.pointsWritten)

		self.SendQueryStats()
	}
	response := &protocol.Response{Type: &endStreamResponse}
	if err != nil {
		message := err.Error()
		response.ErrorMessage = &message
	}
	self.responseChan <- response
}

func (self *QueryEngine) SendQueryStats() {
	timestamp := time.Now().UnixNano() / int64(time.Microsecond)

	runTime := self.runEndTime - self.runStartTime
	points := []*protocol.Point{}
	pointsRead := self.pointsRead
	pointsWritten := self.pointsWritten
	shardId := int64(self.shardId)
	shardLocal := self.shardLocal
	engineName := "QueryEngine"

	point := &protocol.Point{
		Values: []*protocol.FieldValue{
			&protocol.FieldValue{StringValue: &engineName},
			&protocol.FieldValue{Int64Value: &shardId},
			&protocol.FieldValue{BoolValue: &shardLocal},
			&protocol.FieldValue{DoubleValue: &runTime},
			&protocol.FieldValue{Int64Value: &pointsRead},
			&protocol.FieldValue{Int64Value: &pointsWritten},
		},
		Timestamp: &timestamp,
	}
	points = append(points, point)

	seriesName := "explain query"
	series := &protocol.Series{
		Name:   &seriesName,
		Fields: []string{"engine_name", "shard_id", "shard_local", "run_time", "points_read", "points_written"},
		Points: points,
	}
	response := &protocol.Response{Type: &explainQueryResponse, Series: series}
	self.responseChan <- response
}

func containsArithmeticOperators(query *parser.SelectQuery) bool {
	for _, column := range query.GetColumnNames() {
		if column.Type == parser.ValueExpression {
			return true
		}
	}
	return false
}

func (self *QueryEngine) getTimestampFromPoint(point *protocol.Point) int64 {
	multiplier := uint64(*self.duration)
	timestampNanoseconds := uint64(*point.GetTimestampInMicroseconds() * 1000)
	return int64(timestampNanoseconds / multiplier * multiplier / 1000)
}

// Mapper given a point returns a group identifier as the first return
// result and a non-time dependent group (the first group without time)
// as the second result
type Mapper func(*protocol.Point) Group

type PointRange struct {
	startTime int64
	endTime   int64
}

func (self *PointRange) UpdateRange(point *protocol.Point) {
	if *point.Timestamp < self.startTime {
		self.startTime = *point.Timestamp
	}
	if *point.Timestamp > self.endTime {
		self.endTime = *point.Timestamp
	}
}

func crossProduct(values [][][]*protocol.FieldValue) [][]*protocol.FieldValue {
	if len(values) == 0 {
		return [][]*protocol.FieldValue{[]*protocol.FieldValue{}}
	}

	_returnedValues := crossProduct(values[:len(values)-1])
	returnValues := [][]*protocol.FieldValue{}
	for _, v := range values[len(values)-1] {
		for _, values := range _returnedValues {
			returnValues = append(returnValues, append(values, v...))
		}
	}
	return returnValues
}

func (self *QueryEngine) executeCountQueryWithGroupBy(query *parser.SelectQuery, yield func(*protocol.Series) error) error {
	self.aggregateYield = yield
	duration, err := query.GetGroupByClause().GetGroupByTime()
	if err != nil {
		return err
	}

	self.isAggregateQuery = true
	self.duration = duration
	self.aggregators = []Aggregator{}
	self.pointsRange = make(map[string]*PointRange)

	for _, value := range query.GetColumnNames() {
		if !value.IsFunctionCall() {
			continue
		}
		lowerCaseName := strings.ToLower(value.Name)
		initializer := registeredAggregators[lowerCaseName]
		if initializer == nil {
			return common.NewQueryError(common.InvalidArgument, fmt.Sprintf("Unknown function %s", value.Name))
		}
		aggregator, err := initializer(query, value, query.GetGroupByClause().FillValue)
		if err != nil {
			return common.NewQueryError(common.InvalidArgument, fmt.Sprintf("%s", err))
		}
		self.aggregators = append(self.aggregators, aggregator)
	}

	for _, elem := range query.GetGroupByClause().Elems {
		if elem.IsFunctionCall() {
			continue
		}
		self.elems = append(self.elems, elem)
	}

	self.fillWithZero = query.GetGroupByClause().FillWithZero

	self.initializeFields()

	self.trie = NewTrie(len(self.elems), len(self.aggregators))

	err = self.distributeQuery(query, func(series *protocol.Series) error {
		if len(series.Points) == 0 {
			return nil
		}

		return self.aggregateValuesForSeries(series)
	})

	return err
}

func (self *QueryEngine) initializeFields() {
	for _, aggregator := range self.aggregators {
		columnNames := aggregator.ColumnNames()
		self.fields = append(self.fields, columnNames...)
	}

	if self.elems == nil {
		return
	}

	for _, value := range self.elems {
		tempName := value.Name
		self.fields = append(self.fields, tempName)
	}
}

var _count = 0

// We have three types of queries:
//   1. time() without fill
//   2. time() with fill
//   3. no time()
//
// For (1) we flush as soon as a new bucket start, the prefix tree
// keeps track of the other group by columns without the time
// bucket. We reset the state to nil as soon as the given group is
// flushed for the current time bucket. This way we keep the groups
// intact since it's likely to see the same groups again for the next
// time bucket. For (2), we keep track of all group by columns with
// time being the last level in the prefix tree. At the end of the
// query we step through [start time, end time] in self.duration steps
// and get the state from the prefix tree, using default values for
// groups without state in the prefix tree. For the last case we keep
// the groups in the prefix tree and on close() we loop through the
// groups and flush their values with a timestamp equal to now()
func (self *QueryEngine) aggregateValuesForSeries(series *protocol.Series) error {
	for _, aggregator := range self.aggregators {
		if err := aggregator.InitializeFieldsMetadata(series); err != nil {
			return err
		}
	}

	currentRange := self.pointsRange[*series.Name]
	if currentRange == nil {
		currentRange = &PointRange{*series.Points[0].Timestamp, *series.Points[0].Timestamp}
		self.pointsRange[*series.Name] = currentRange
	}
	group := make([]*protocol.FieldValue, len(self.elems))
	for _, point := range series.Points {
		currentRange.UpdateRange(point)

		// this is a groupby with time() and no fill, flush as soon as we
		// start a new bucket
		if self.duration != nil && !self.fillWithZero {
			// this is the timestamp aggregator
			timestamp := self.getTimestampFromPoint(point)
			if self.lastTimestamp != timestamp {
				self.runAggregatesForTable(series.GetName())
			}
			self.lastTimestamp = timestamp
		}

		self.started = true

		// get the group this point belongs to
		for idx, elem := range self.elems {
			// TODO: create an index from fieldname to index
			value, err := GetValue(elem, series.Fields, point)
			if err != nil {
				return err
			}
			group[idx] = value
		}

		// update the state of the given group
		node := self.trie.GetNode(group)
		_count++
		if _count%100000 == 0 {
			fmt.Printf("Trie size: %d\n", self.trie.GetSize())
		}
		var err error
		for idx, aggregator := range self.aggregators {
			node.states[idx], err = aggregator.AggregatePoint(node.states[idx], point)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (self *QueryEngine) runAggregates() {
	// TODO: make sure this logic work for multiple tables
	self.runAggregatesForTable("")
}

func (self *QueryEngine) calculateSummariesForTable(table string, timestamp int64) {
	// TODO: make sure this logic work for multiple tables

	err := self.trie.Traverse(func(_ []*protocol.FieldValue, node *Node) error {
		for idx, aggregator := range self.aggregators {
			aggregator.CalculateSummaries(node.states[idx])
		}
		return nil
	})
	if err != nil {
		panic("Error while calculating summaries")
	}
}

func (self *QueryEngine) runAggregatesForTable(table string) {
	// TODO: if this is a fill query, step through [start,end] in duration
	// steps and flush the groups for the given bucket

	points := make([]*protocol.Point, 0, self.trie.GetSize())
	err := self.trie.Traverse(func(group []*protocol.FieldValue, node *Node) error {
		fmt.Printf("Group: %v\n", group)
		points = append(points, self.getValuesForGroup(group, node)...)
		return nil
	})
	if err != nil {
		panic(err)
	}
	self.aggregateYield(&protocol.Series{
		Name: &table,
	})
}

func (self *QueryEngine) getValuesForGroup(group []*protocol.FieldValue, node *Node) []*protocol.Point {

	values := [][][]*protocol.FieldValue{}

	var timestamp int64
	useTimestamp := false
	if self.duration != nil {
		// if there's a group by time(), then the timestamp is the lastTimestamp
		timestamp = self.lastTimestamp
		useTimestamp = true
	} else if self.fillWithZero {
		// if there's no group by time(), but a fill value was specified,
		// the timestamp is the last value in the group
		timestamp = group[len(group)-1].GetInt64Value()
		useTimestamp = true
	}

	for idx, aggregator := range self.aggregators {
		values = append(values, aggregator.GetValues(node.states[idx]))
	}

	fmt.Printf("state: %v\n", node.states)

	// do cross product of all the values
	_values := crossProduct(values)

	points := []*protocol.Point{}

	for _, v := range _values {
		/* groupPoints := []*protocol.Point{} */
		point := &protocol.Point{
			Values: v,
		}

		if useTimestamp {
			point.SetTimestampInMicroseconds(timestamp)
		} else {
			point.SetTimestampInMicroseconds(time.Now().Unix() * 1000000)
		}

		// FIXME: this should be looking at the fields slice not the group by clause
		// FIXME: we should check whether the selected columns are in the group by clause
		for idx, _ := range self.elems {
			if self.duration != nil && idx == 0 {
				continue
			}

			point.Values = append(point.Values, group[idx])
		}

		points = append(points, point)
	}
	return points
}

func (self *QueryEngine) executeArithmeticQuery(query *parser.SelectQuery, yield func(*protocol.Series) error) error {

	names := map[string]*parser.Value{}
	for idx, v := range query.GetColumnNames() {
		switch v.Type {
		case parser.ValueSimpleName:
			names[v.Name] = v
		case parser.ValueFunctionCall:
			names[v.Name] = v
		case parser.ValueExpression:
			names["expr"+strconv.Itoa(idx)] = v
		}
	}

	return self.distributeQuery(query, func(series *protocol.Series) error {
		if len(series.Points) == 0 {
			yield(series)
			return nil
		}

		newSeries := &protocol.Series{
			Name: series.Name,
		}

		// create the new column names
		for name, _ := range names {
			newSeries.Fields = append(newSeries.Fields, name)
		}

		for _, point := range series.Points {
			newPoint := &protocol.Point{
				Timestamp:      point.Timestamp,
				SequenceNumber: point.SequenceNumber,
			}
			for _, field := range newSeries.Fields {
				value := names[field]
				v, err := GetValue(value, series.Fields, point)
				if err != nil {
					log.Error("Error in arithmetic computation: %s", err)
					return err
				}
				newPoint.Values = append(newPoint.Values, v)
			}
			newSeries.Points = append(newSeries.Points, newPoint)
		}

		yield(newSeries)

		return nil
	})
}

func (self *QueryEngine) GetName() string {
	return "QueryEngine"
}
