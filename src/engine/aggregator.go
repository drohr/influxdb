package engine

import (
	"common"
	"fmt"
	"math"
	"parser"
	"protocol"
	"sort"
	"strconv"
	"strings"
	"time"
)

type PointSlice []protocol.Point

type Aggregator interface {
	AggregatePoint(series string, group interface{}, p *protocol.Point) error
	AggregateSeries(series string, group interface{}, s *protocol.Series) error
	InitializeFieldsMetadata(series *protocol.Series) error
	GetValues(series string, group interface{}) [][]*protocol.FieldValue
	CalculateSummaries(series string, group interface{})
	ColumnNames() []string
}

// Initialize a new aggregator given the query, the function call of
// the aggregator and the default value that should be returned if
// the bucket doesn't have any points
type AggregatorInitializer func(*parser.SelectQuery, *parser.Value, *parser.Value) (Aggregator, error)

var registeredAggregators = make(map[string]AggregatorInitializer)

func init() {
	registeredAggregators["count"] = NewCountAggregator
	registeredAggregators["histogram"] = NewHistogramAggregator
	registeredAggregators["derivative"] = NewDerivativeAggregator
	registeredAggregators["stddev"] = NewStandardDeviationAggregator
	registeredAggregators["max"] = NewMaxAggregator
	registeredAggregators["min"] = NewMinAggregator
	registeredAggregators["sum"] = NewSumAggregator
	registeredAggregators["percentile"] = NewPercentileAggregator
	registeredAggregators["median"] = NewMedianAggregator
	registeredAggregators["mean"] = NewMeanAggregator
	registeredAggregators["mode"] = NewModeAggregator
	registeredAggregators["distinct"] = NewDistinctAggregator
	registeredAggregators["first"] = NewFirstAggregator
	registeredAggregators["last"] = NewLastAggregator
}

// used in testing to get a list of all aggregators
func GetRegisteredAggregators() (names []string) {
	for n, _ := range registeredAggregators {
		names = append(names, n)
	}
	return
}

type AbstractAggregator struct {
	Aggregator
	value   *parser.Value
	columns []string
}

func (self *AbstractAggregator) InitializeFieldsMetadata(series *protocol.Series) error {
	self.columns = series.Fields
	return nil
}

func (self *AbstractAggregator) CalculateSummaries(series string, group interface{}) {
}

//
// Composite Aggregator
//

type CompositeAggregator struct {
	left  Aggregator
	right Aggregator
}

func (self *CompositeAggregator) AggregatePoint(series string, group interface{}, p *protocol.Point) error {
	return self.right.AggregatePoint(series, group, p)
}

func (self *CompositeAggregator) AggregateSeries(series string, group interface{}, s *protocol.Series) error {
	return self.right.AggregateSeries(series, group, s)
}

func (self *CompositeAggregator) ColumnNames() []string {
	return self.left.ColumnNames()
}

func (self *CompositeAggregator) CalculateSummaries(series string, group interface{}) {
	self.right.CalculateSummaries(series, group)
	values := self.right.GetValues(series, group)
	for _, v := range values {
		point := &protocol.Point{Values: v}
		self.left.AggregatePoint(series, group, point)
	}
	self.left.CalculateSummaries(series, group)
}

func (self *CompositeAggregator) GetValues(series string, group interface{}) [][]*protocol.FieldValue {
	values := self.left.GetValues(series, group)
	return values
}

func (self *CompositeAggregator) InitializeFieldsMetadata(series *protocol.Series) error {
	return self.right.InitializeFieldsMetadata(series)
}

func NewCompositeAggregator(left, right Aggregator) (Aggregator, error) {
	return &CompositeAggregator{left, right}, nil
}

//
// StandardDeviation Aggregator
//

type StandardDeviationRunning struct {
	count   int
	totalX2 float64
	totalX  float64
}

type StandardDeviationAggregator struct {
	AbstractAggregator
	running      map[string]map[interface{}]*StandardDeviationRunning
	defaultValue *protocol.FieldValue
	alias        string
}

func (self *StandardDeviationAggregator) AggregatePoint(series string, group interface{}, p *protocol.Point) error {
	fieldValue, err := GetValue(self.value, self.columns, p)
	if err != nil {
		return err
	}

	var value float64
	if ptr := fieldValue.Int64Value; ptr != nil {
		value = float64(*ptr)
	} else if ptr := fieldValue.DoubleValue; ptr != nil {
		value = *ptr
	} else {
		// else ignore this point
		return nil
	}

	running := self.running[series]
	if running == nil {
		running = make(map[interface{}]*StandardDeviationRunning)
		self.running[series] = running
	}

	r := running[group]
	if r == nil {
		r = &StandardDeviationRunning{}
		running[group] = r
	}

	r.count++
	r.totalX += value
	r.totalX2 += value * value
	return nil
}

//TODO: to be optimized
func (self *StandardDeviationAggregator) AggregateSeries(series string, group interface{}, s *protocol.Series) error {
	for _, p := range s.Points {
		self.AggregatePoint(series, group, p)
	}
	return nil
}

func (self *StandardDeviationAggregator) ColumnNames() []string {
	if self.alias != "" {
		return []string{self.alias}
	}

	return []string{"stddev"}
}

func (self *StandardDeviationAggregator) GetValues(series string, group interface{}) [][]*protocol.FieldValue {
	r := self.running[series][group]

	defer delete(self.running[series], group)

	if r.count == 0 {
		return [][]*protocol.FieldValue{
			[]*protocol.FieldValue{self.defaultValue},
		}
	}

	eX := r.totalX / float64(r.count)
	eX *= eX
	eX2 := r.totalX2 / float64(r.count)
	standardDeviation := math.Sqrt(eX2 - eX)

	return [][]*protocol.FieldValue{
		[]*protocol.FieldValue{
			&protocol.FieldValue{DoubleValue: &standardDeviation},
		},
	}
}

func wrapDefaultValue(defaultValue *parser.Value) (*protocol.FieldValue, error) {
	if defaultValue == nil {
		return nil, nil
	}

	switch defaultValue.Type {
	case parser.ValueInt:
		v, _ := strconv.Atoi(defaultValue.Name)
		value := int64(v)
		return &protocol.FieldValue{Int64Value: &value}, nil
	default:
		return nil, fmt.Errorf("Unknown type %s", defaultValue.Type)
	}
}

func NewStandardDeviationAggregator(q *parser.SelectQuery, v *parser.Value, defaultValue *parser.Value) (Aggregator, error) {
	if len(v.Elems) != 1 {
		return nil, common.NewQueryError(common.WrongNumberOfArguments, "function stddev() requires exactly one argument")
	}

	if v.Elems[0].Type == parser.ValueWildcard {
		return nil, common.NewQueryError(common.InvalidArgument, "function stddev() doesn't work with wildcards")
	}

	value, err := wrapDefaultValue(defaultValue)
	if err != nil {
		return nil, err
	}
	return &StandardDeviationAggregator{
		AbstractAggregator: AbstractAggregator{
			value: v.Elems[0],
		},
		running:      make(map[string]map[interface{}]*StandardDeviationRunning),
		defaultValue: value,
		alias:        v.Alias,
	}, nil
}

//
// Derivative Aggregator
//

type DerivativeAggregator struct {
	AbstractAggregator
	firstValues  map[string]map[interface{}]*protocol.Point
	lastValues   map[string]map[interface{}]*protocol.Point
	defaultValue *protocol.FieldValue
	alias        string
}

func (self *DerivativeAggregator) AggregatePoint(series string, group interface{}, p *protocol.Point) error {
	fieldValue, err := GetValue(self.value, self.columns, p)
	if err != nil {
		return err
	}

	var value float64
	if ptr := fieldValue.Int64Value; ptr != nil {
		value = float64(*ptr)
	} else if ptr := fieldValue.DoubleValue; ptr != nil {
		value = *ptr
	} else {
		// else ignore this point
		return nil
	}

	newValue := &protocol.Point{
		Timestamp: p.Timestamp,
		Values:    []*protocol.FieldValue{&protocol.FieldValue{DoubleValue: &value}},
	}

	firstValues := self.firstValues[series]
	if firstValues == nil {
		firstValues = make(map[interface{}]*protocol.Point)
		self.firstValues[series] = firstValues
	}

	if _, ok := firstValues[group]; !ok {
		firstValues[group] = newValue
		return nil
	}

	lastValues := self.lastValues[series]
	if lastValues == nil {
		lastValues = make(map[interface{}]*protocol.Point)
		self.lastValues[series] = lastValues
	}

	lastValues[group] = newValue
	return nil
}

//TODO: to be optimized
func (self *DerivativeAggregator) AggregateSeries(series string, group interface{}, s *protocol.Series) error {
	for _, p := range s.Points {
		self.AggregatePoint(series, group, p)
	}
	return nil
}

func (self *DerivativeAggregator) ColumnNames() []string {
	if self.alias != "" {
		return []string{self.alias}
	}
	return []string{"derivative"}
}

func (self *DerivativeAggregator) GetValues(series string, group interface{}) [][]*protocol.FieldValue {
	oldValue := self.firstValues[series][group]
	newValue := self.lastValues[series][group]

	defer delete(self.firstValues[series], group)
	defer delete(self.lastValues[series], group)

	if newValue != nil {
		// if an old value exist, then compute the derivative and insert it in the points slice
		deltaT := float64(*newValue.Timestamp-*oldValue.Timestamp) / float64(time.Second/time.Microsecond)
		deltaV := *newValue.Values[0].DoubleValue - *oldValue.Values[0].DoubleValue
		derivative := deltaV / deltaT
		return [][]*protocol.FieldValue{
			[]*protocol.FieldValue{
				&protocol.FieldValue{DoubleValue: &derivative},
			},
		}
	} else if self.defaultValue != nil {
		return [][]*protocol.FieldValue{
			[]*protocol.FieldValue{self.defaultValue},
		}
	}
	return [][]*protocol.FieldValue{}
}

func NewDerivativeAggregator(q *parser.SelectQuery, v *parser.Value, defaultValue *parser.Value) (Aggregator, error) {
	if len(v.Elems) != 1 {
		return nil, common.NewQueryError(common.WrongNumberOfArguments, "function derivative() requires exactly one argument")
	}

	if v.Elems[0].Type == parser.ValueWildcard {
		return nil, common.NewQueryError(common.InvalidArgument, "function derivative() doesn't work with wildcards")
	}

	wrappedDefaultValue, err := wrapDefaultValue(defaultValue)
	if err != nil {
		return nil, err
	}

	return &DerivativeAggregator{
		AbstractAggregator: AbstractAggregator{
			value: v.Elems[0],
		},
		firstValues:  make(map[string]map[interface{}]*protocol.Point),
		lastValues:   make(map[string]map[interface{}]*protocol.Point),
		defaultValue: wrappedDefaultValue,
		alias:        v.Alias,
	}, nil
}

//
// Histogram Aggregator
//

type HistogramAggregator struct {
	AbstractAggregator
	bucketSize  float64
	histograms  map[string]map[interface{}]map[int]int
	columnNames []string
}

func (self *HistogramAggregator) AggregatePoint(series string, group interface{}, p *protocol.Point) error {
	groups := self.histograms[series]
	if groups == nil {
		groups = make(map[interface{}]map[int]int)
		self.histograms[series] = groups
	}

	buckets := groups[group]
	if buckets == nil {
		buckets = make(map[int]int)
		groups[group] = buckets
	}

	fieldValue, err := GetValue(self.value, self.columns, p)
	if err != nil {
		return err
	}

	var value float64
	if ptr := fieldValue.Int64Value; ptr != nil {
		value = float64(*ptr)
	} else if ptr := fieldValue.DoubleValue; ptr != nil {
		value = *ptr
	}

	bucket := int(value / self.bucketSize)
	buckets[bucket] += 1

	return nil
}

//TODO: to be optimized
func (self *HistogramAggregator) AggregateSeries(series string, group interface{}, s *protocol.Series) error {
	for _, p := range s.Points {
		self.AggregatePoint(series, group, p)
	}
	return nil
}

func (self *HistogramAggregator) ColumnNames() []string {
	return self.columnNames
}

func (self *HistogramAggregator) GetValues(series string, group interface{}) [][]*protocol.FieldValue {
	returnValues := [][]*protocol.FieldValue{}
	buckets := self.histograms[series][group]
	defer delete(self.histograms[series], group)
	for bucket, size := range buckets {
		_bucket := float64(bucket) * self.bucketSize
		_size := int64(size)

		returnValues = append(returnValues, []*protocol.FieldValue{
			&protocol.FieldValue{DoubleValue: &_bucket},
			&protocol.FieldValue{Int64Value: &_size},
		})
	}

	return returnValues
}

func NewHistogramAggregator(q *parser.SelectQuery, v *parser.Value, defaultValue *parser.Value) (Aggregator, error) {
	if len(v.Elems) < 1 {
		return nil, common.NewQueryError(common.WrongNumberOfArguments, "function histogram() requires at least one arguments")
	}

	if len(v.Elems) > 2 {
		return nil, common.NewQueryError(common.WrongNumberOfArguments, "function histogram() takes at most two arguments")
	}

	if v.Elems[0].Type == parser.ValueWildcard {
		return nil, common.NewQueryError(common.InvalidArgument, "function histogram() doesn't work with wildcards")
	}

	bucketSize := 1.0

	if len(v.Elems) == 2 {
		switch v.Elems[1].Type {
		case parser.ValueInt, parser.ValueFloat:
			var err error
			bucketSize, err = strconv.ParseFloat(v.Elems[1].Name, 64)
			if err != nil {
				return nil, common.NewQueryError(common.InvalidArgument, "Cannot parse %s into a float", v.Elems[1].Name)
			}
		default:
			return nil, common.NewQueryError(common.InvalidArgument, "Cannot parse %s into a float", v.Elems[1].Name)
		}
	}

	columnNames := []string{"bucket_start", "count"}
	if v.Alias != "" {
		columnNames[0] = fmt.Sprintf("%s_bucket_start", v.Alias)
		columnNames[1] = fmt.Sprintf("%s_count", v.Alias)
	}

	return &HistogramAggregator{
		AbstractAggregator: AbstractAggregator{
			value: v.Elems[0],
		},
		bucketSize:  bucketSize,
		histograms:  make(map[string]map[interface{}]map[int]int),
		columnNames: columnNames,
	}, nil
}

//
// Count Aggregator
//

type CountAggregator struct {
	AbstractAggregator
	defaultValue *protocol.FieldValue
	counts       map[string]map[interface{}]int64
	alias        string
}

func (self *CountAggregator) AggregatePoint(series string, group interface{}, p *protocol.Point) error {
	counts := self.counts[series]
	if counts == nil {
		counts = make(map[interface{}]int64)
		self.counts[series] = counts
	}
	counts[group]++
	return nil
}

func (self *CountAggregator) AggregateSeries(series string, group interface{}, s *protocol.Series) error {
	counts := self.counts[series]
	if counts == nil {
		counts = make(map[interface{}]int64)
		self.counts[series] = counts
	}
	counts[group] += int64(len(s.Points))
	return nil
}

func (self *CountAggregator) ColumnNames() []string {
	if self.alias != "" {
		return []string{self.alias}
	}
	return []string{"count"}
}

func (self *CountAggregator) GetValues(series string, group interface{}) [][]*protocol.FieldValue {
	returnValues := [][]*protocol.FieldValue{}
	_value, ok := self.counts[series][group]
	defer delete(self.counts[series], group)
	value := int64(_value)
	if !ok {
		returnValues = append(returnValues, []*protocol.FieldValue{self.defaultValue})
	} else {
		returnValues = append(returnValues, []*protocol.FieldValue{
			&protocol.FieldValue{Int64Value: &value},
		})
	}

	return returnValues
}

func (self *CountAggregator) InitializeFieldsMetadata(series *protocol.Series) error { return nil }

func NewCountAggregator(q *parser.SelectQuery, v *parser.Value, defaultValue *parser.Value) (Aggregator, error) {
	if len(v.Elems) != 1 {
		return nil, common.NewQueryError(common.WrongNumberOfArguments, "function count() requires exactly one argument")
	}

	if v.Elems[0].Type == parser.ValueWildcard {
		return nil, common.NewQueryError(common.InvalidArgument, "function count() doesn't work with wildcards")
	}

	wrappedDefaultValue, err := wrapDefaultValue(defaultValue)
	if err != nil {
		return nil, err
	}

	if v.Elems[0].Type != parser.ValueSimpleName {
		innerName := strings.ToLower(v.Elems[0].Name)
		init := registeredAggregators[innerName]
		if init == nil {
			return nil, common.NewQueryError(common.InvalidArgument, fmt.Sprintf("Unknown function %s", innerName))
		}
		inner, err := init(q, v.Elems[0], defaultValue)
		if err != nil {
			return nil, err
		}
		return NewCompositeAggregator(&CountAggregator{AbstractAggregator{}, wrappedDefaultValue, make(map[string]map[interface{}]int64), v.Alias}, inner)
	}

	return &CountAggregator{AbstractAggregator{}, wrappedDefaultValue, make(map[string]map[interface{}]int64), v.Alias}, nil
}

//
// Timestamp Aggregator
//

type TimestampAggregator struct {
	AbstractAggregator
	duration   *uint64
	timestamps map[string]map[interface{}]int64
}

func (self *TimestampAggregator) AggregatePoint(series string, group interface{}, p *protocol.Point) error {
	timestamps := self.timestamps[series]
	if timestamps == nil {
		timestamps = make(map[interface{}]int64)
		self.timestamps[series] = timestamps
	}
	if self.duration != nil {
		timestampNanoseconds := uint64(*p.GetTimestampInMicroseconds()) * 1000
		timestamps[group] = int64(timestampNanoseconds / *self.duration * *self.duration / 1000)
	} else {
		timestamps[group] = *p.GetTimestampInMicroseconds()
	}
	return nil
}

func (self *TimestampAggregator) AggregateSeries(series string, group interface{}, s *protocol.Series) error {
	if len(s.Points) == 0 {
		return nil
	}

	return self.AggregatePoint(series, group, s.Points[len(s.Points)-1])
}

/*
//TODO: to be optimized
func (self *TimestampAggregator) AggregateSeries(series string, group interface{}, s *protocol.Series) error {
	//log.Error("Timestamp: ", len(s.Points))
	for _, p := range s.Points {
		//log.Error("Point: ", p)
		self.AggregatePoint(series, group, p)
	}
	return nil
}
*/

func (self *TimestampAggregator) ColumnNames() []string {
	return []string{"count"}
}

func (self *TimestampAggregator) GetValues(series string, group interface{}) [][]*protocol.FieldValue {
	return self.GetValuesOptionalDelete(series, group, true)
}

func (self *TimestampAggregator) GetValuesOptionalDelete(series string, group interface{}, deleteValue bool) [][]*protocol.FieldValue {
	returnValues := [][]*protocol.FieldValue{}
	value := self.timestamps[series][group]
	if deleteValue {
		defer delete(self.timestamps[series], group)
	}
	returnValues = append(returnValues, []*protocol.FieldValue{
		&protocol.FieldValue{Int64Value: &value},
	})

	return returnValues
}

func (self *TimestampAggregator) InitializeFieldsMetadata(series *protocol.Series) error { return nil }

func NewTimestampAggregator(query *parser.SelectQuery, _ *parser.Value) (*TimestampAggregator, error) {
	duration, err := query.GetGroupByClause().GetGroupByTime()
	if err != nil {
		return nil, err
	}

	var durationPtr *uint64

	if duration != nil {
		newDuration := uint64(*duration)
		durationPtr = &newDuration
	}

	return &TimestampAggregator{
		AbstractAggregator: AbstractAggregator{},
		timestamps:         make(map[string]map[interface{}]int64),
		duration:           durationPtr,
	}, nil
}

//
// Mean Aggregator
//

type MeanAggregator struct {
	AbstractAggregator
	means        map[string]map[interface{}]float64
	counts       map[string]map[interface{}]int
	defaultValue *protocol.FieldValue
	alias        string
}

func (self *MeanAggregator) AggregatePoint(series string, group interface{}, p *protocol.Point) error {
	means := self.means[series]
	counts := self.counts[series]

	if means == nil && counts == nil {
		means = make(map[interface{}]float64)
		self.means[series] = means

		counts = make(map[interface{}]int)
		self.counts[series] = counts
	}

	currentMean := means[group]
	currentCount := counts[group] + 1

	fieldValue, err := GetValue(self.value, self.columns, p)
	if err != nil {
		return err
	}

	var value float64
	if ptr := fieldValue.Int64Value; ptr != nil {
		value = float64(*ptr)
	} else if ptr := fieldValue.DoubleValue; ptr != nil {
		value = *ptr
	}

	currentMean = currentMean*float64(currentCount-1)/float64(currentCount) + value/float64(currentCount)

	means[group] = currentMean
	counts[group] = currentCount
	return nil
}

//TODO: to be optimized
func (self *MeanAggregator) AggregateSeries(series string, group interface{}, s *protocol.Series) error {
	for _, p := range s.Points {
		self.AggregatePoint(series, group, p)
	}
	return nil
}

func (self *MeanAggregator) ColumnNames() []string {
	if self.alias != "" {
		return []string{self.alias}
	}
	return []string{"mean"}
}

func (self *MeanAggregator) GetValues(series string, group interface{}) [][]*protocol.FieldValue {
	returnValues := [][]*protocol.FieldValue{}
	mean, ok := self.means[series][group]
	defer delete(self.means[series], group)
	if !ok {
		returnValues = append(returnValues, []*protocol.FieldValue{self.defaultValue})
	} else {
		returnValues = append(returnValues, []*protocol.FieldValue{
			&protocol.FieldValue{DoubleValue: &mean},
		})
	}

	return returnValues
}

func NewMeanAggregator(_ *parser.SelectQuery, value *parser.Value, defaultValue *parser.Value) (Aggregator, error) {
	if len(value.Elems) != 1 {
		return nil, common.NewQueryError(common.WrongNumberOfArguments, "function mean() requires exactly one argument")
	}

	wrappedDefaultValue, err := wrapDefaultValue(defaultValue)
	if err != nil {
		return nil, err
	}

	return &MeanAggregator{
		AbstractAggregator: AbstractAggregator{
			value: value.Elems[0],
		},
		means:        make(map[string]map[interface{}]float64),
		counts:       make(map[string]map[interface{}]int),
		defaultValue: wrappedDefaultValue,
		alias:        value.Alias,
	}, nil
}

func NewMedianAggregator(_ *parser.SelectQuery, value *parser.Value, defaultValue *parser.Value) (Aggregator, error) {
	if len(value.Elems) != 1 {
		return nil, common.NewQueryError(common.WrongNumberOfArguments, "function median() requires exactly one argument")
	}

	wrappedDefaultValue, err := wrapDefaultValue(defaultValue)
	if err != nil {
		return nil, err
	}

	functionName := "median"
	if value.Alias != "" {
		functionName = value.Alias
	}

	aggregator := &PercentileAggregator{
		AbstractAggregator: AbstractAggregator{
			value: value.Elems[0],
		},
		functionName: functionName,
		percentile:   50.0,
		percentiles:  make(map[string]map[interface{}]float64),
		state:        make(map[string]map[interface{}][]float64),
		defaultValue: wrappedDefaultValue,
		alias:        value.Alias,
	}
	return aggregator, nil
}

//
// Percentile Aggregator
//

type PercentileAggregator struct {
	AbstractAggregator
	functionName string
	percentile   float64
	percentiles  map[string]map[interface{}]float64
	state        map[string]map[interface{}][]float64
	defaultValue *protocol.FieldValue
	alias        string
}

func (self *PercentileAggregator) AggregatePoint(series string, group interface{}, p *protocol.Point) error {
	v, err := GetValue(self.value, self.columns, p)
	if err != nil {
		return err
	}

	value := 0.0
	if v.Int64Value != nil {
		value = float64(*v.Int64Value)
	} else if v.DoubleValue != nil {
		value = *v.DoubleValue
	} else {
		return nil
	}

	values := self.state[series]
	if values == nil {
		values = map[interface{}][]float64{}
		self.state[series] = values
	}

	values[group] = append(values[group], value)

	return nil
}

//TODO: to be optimized
func (self *PercentileAggregator) AggregateSeries(series string, group interface{}, s *protocol.Series) error {
	for _, p := range s.Points {
		self.AggregatePoint(series, group, p)
	}
	return nil
}

func (self *PercentileAggregator) ColumnNames() []string {
	if self.alias != "" {
		return []string{self.alias}
	}
	return []string{self.functionName}
}

func (self *PercentileAggregator) GetValues(series string, group interface{}) [][]*protocol.FieldValue {
	value, ok := self.percentiles[series][group]
	defer delete(self.percentiles[series], group)

	if !ok {
		return [][]*protocol.FieldValue{
			[]*protocol.FieldValue{self.defaultValue},
		}
	}

	return [][]*protocol.FieldValue{
		[]*protocol.FieldValue{&protocol.FieldValue{DoubleValue: &value}},
	}
}

func (self *PercentileAggregator) CalculateSummaries(series string, group interface{}) {
	values := self.state[series][group]
	defer delete(self.state[series], group)
	sort.Float64s(values)
	length := len(values)
	index := int(math.Floor(float64(length)*self.percentile/100.0+0.5)) - 1

	if index < 0 || index >= len(values) {
		return
	}

	value := values[index]
	percentiles := self.percentiles[series]
	if percentiles == nil {
		percentiles = map[interface{}]float64{}
		self.percentiles[series] = percentiles
	}
	percentiles[group] = value
}

func NewPercentileAggregator(_ *parser.SelectQuery, value *parser.Value, defaultValue *parser.Value) (Aggregator, error) {
	if len(value.Elems) != 2 {
		return nil, common.NewQueryError(common.WrongNumberOfArguments, "function percentile() requires exactly two arguments")
	}
	percentile, err := strconv.ParseFloat(value.Elems[1].Name, 64)

	if err != nil || percentile <= 0 || percentile >= 100 {
		return nil, common.NewQueryError(common.InvalidArgument, "function percentile() requires a numeric second argument between 0 and 100")
	}

	wrappedDefaultValue, err := wrapDefaultValue(defaultValue)
	if err != nil {
		return nil, err
	}

	functionName := "percentile"
	if value.Alias != "" {
		functionName = value.Alias
	}

	return &PercentileAggregator{
		AbstractAggregator: AbstractAggregator{
			value: value.Elems[0],
		},
		functionName: functionName,
		percentile:   percentile,
		percentiles:  make(map[string]map[interface{}]float64),
		state:        make(map[string]map[interface{}][]float64),
		defaultValue: wrappedDefaultValue,
	}, nil
}

//
// Mode Aggregator
//

type ModeAggregator struct {
	AbstractAggregator
	counts       map[string]map[interface{}]map[float64]int
	modes        map[string]map[interface{}][]float64
	defaultValue *protocol.FieldValue
	alias        string
}

func (self *ModeAggregator) AggregatePoint(series string, group interface{}, p *protocol.Point) error {
	seriesCounts := self.counts[series]
	if seriesCounts == nil {
		seriesCounts = make(map[interface{}]map[float64]int)
		self.counts[series] = seriesCounts
	}

	groupCounts := seriesCounts[group]
	if groupCounts == nil {
		groupCounts = make(map[float64]int)
	}

	point, err := GetValue(self.value, self.columns, p)
	if err != nil {
		return err
	}

	var value float64
	if point.Int64Value != nil {
		value = float64(*point.Int64Value)
	} else if point.DoubleValue != nil {
		value = *point.DoubleValue
	} else {
		return nil
	}

	count := groupCounts[value]
	count += 1
	groupCounts[value] = count
	seriesCounts[group] = groupCounts

	return nil
}

//TODO: to be optimized
func (self *ModeAggregator) AggregateSeries(series string, group interface{}, s *protocol.Series) error {
	for _, p := range s.Points {
		self.AggregatePoint(series, group, p)
	}
	return nil
}

func (self *ModeAggregator) ColumnNames() []string {
	if self.alias != "" {
		return []string{self.alias}
	}
	return []string{"mode"}
}

func (self *ModeAggregator) CalculateSummaries(series string, group interface{}) {
	modes := []float64{}
	currentCount := 1

	defer delete(self.counts[series], group)
	for value, count := range self.counts[series][group] {
		if count == currentCount {
			modes = append(modes, value)
		} else if count > currentCount {
			modes = nil
			modes = append(modes, value)
			currentCount = count
		}
	}

	seriesModes := self.modes[series]
	if seriesModes == nil {
		seriesModes = map[interface{}][]float64{}
		self.modes[series] = seriesModes
	}

	seriesModes[group] = modes
}

func (self *ModeAggregator) GetValues(series string, group interface{}) [][]*protocol.FieldValue {
	returnValues := [][]*protocol.FieldValue{}

	modes := self.modes[series][group]
	defer delete(self.modes[series], group)

	for _, value := range modes {
		// we can't use value since we need a pointer to a variable that won't change,
		// while value will change the next iteration
		v := value
		returnValues = append(returnValues, []*protocol.FieldValue{
			&protocol.FieldValue{DoubleValue: &v},
		})
	}

	if len(modes) == 0 {
		returnValues = append(returnValues, []*protocol.FieldValue{self.defaultValue})
	}

	return returnValues
}

func NewModeAggregator(_ *parser.SelectQuery, value *parser.Value, defaultValue *parser.Value) (Aggregator, error) {
	if len(value.Elems) != 1 {
		return nil, common.NewQueryError(common.WrongNumberOfArguments, "function mode() requires exactly one argument")
	}

	wrappedDefaultValue, err := wrapDefaultValue(defaultValue)
	if err != nil {
		return nil, err
	}

	return &ModeAggregator{
		AbstractAggregator: AbstractAggregator{
			value: value.Elems[0],
		},
		counts:       make(map[string]map[interface{}]map[float64]int),
		modes:        make(map[string]map[interface{}][]float64),
		defaultValue: wrappedDefaultValue,
		alias:        value.Alias,
	}, nil
}

//
// Distinct Aggregator
//

type DistinctAggregator struct {
	AbstractAggregator
	counts       map[string]map[interface{}]map[interface{}]int
	defaultValue *protocol.FieldValue
	alias        string
}

func (self *DistinctAggregator) AggregatePoint(series string, group interface{}, p *protocol.Point) error {
	seriesCounts := self.counts[series]
	if seriesCounts == nil {
		seriesCounts = make(map[interface{}]map[interface{}]int)
		self.counts[series] = seriesCounts
	}

	groupCounts := seriesCounts[group]
	if groupCounts == nil {
		groupCounts = make(map[interface{}]int)
		seriesCounts[group] = groupCounts
	}

	point, err := GetValue(self.value, self.columns, p)
	if err != nil {
		return err
	}

	var value interface{}
	if point.Int64Value != nil {
		value = float64(*point.Int64Value)
	} else if point.DoubleValue != nil {
		value = *point.DoubleValue
	} else if point.BoolValue != nil {
		value = *point.BoolValue
	} else if point.StringValue != nil {
		value = *point.StringValue
	} else {
		return nil
	}

	groupCounts[value]++

	return nil
}

//TODO: to be optimized
func (self *DistinctAggregator) AggregateSeries(series string, group interface{}, s *protocol.Series) error {
	for _, p := range s.Points {
		self.AggregatePoint(series, group, p)
	}
	return nil
}

func (self *DistinctAggregator) ColumnNames() []string {
	if self.alias != "" {
		return []string{self.alias}
	}
	return []string{"distinct"}
}

func (self *DistinctAggregator) GetValues(series string, group interface{}) [][]*protocol.FieldValue {
	returnValues := [][]*protocol.FieldValue{}

	values := self.counts[series][group]
	defer delete(self.counts[series], group)

	if len(values) == 0 {
		returnValues = append(returnValues, []*protocol.FieldValue{self.defaultValue})
	}

	for value, _ := range values {
		switch v := value.(type) {
		case int:
			i := int64(v)
			returnValues = append(returnValues, []*protocol.FieldValue{&protocol.FieldValue{Int64Value: &i}})
		case string:
			returnValues = append(returnValues, []*protocol.FieldValue{&protocol.FieldValue{StringValue: &v}})
		case bool:
			returnValues = append(returnValues, []*protocol.FieldValue{&protocol.FieldValue{BoolValue: &v}})
		case float64:
			returnValues = append(returnValues, []*protocol.FieldValue{&protocol.FieldValue{DoubleValue: &v}})
		}
	}

	return returnValues
}

func NewDistinctAggregator(_ *parser.SelectQuery, value *parser.Value, defaultValue *parser.Value) (Aggregator, error) {
	wrappedDefaultValue, err := wrapDefaultValue(defaultValue)
	if err != nil {
		return nil, err
	}

	return &DistinctAggregator{
		AbstractAggregator: AbstractAggregator{
			value: value.Elems[0],
		},
		counts:       make(map[string]map[interface{}]map[interface{}]int),
		defaultValue: wrappedDefaultValue,
		alias:        value.Alias,
	}, nil
}

//
// Max, Min and Sum Aggregators
//

type Operation func(currentValue float64, newValue *protocol.FieldValue) float64

type CumulativeArithmeticAggregator struct {
	AbstractAggregator
	name         string
	values       map[string]map[interface{}]float64
	operation    Operation
	initialValue float64
	defaultValue *protocol.FieldValue
}

func (self *CumulativeArithmeticAggregator) AggregatePoint(series string, group interface{}, p *protocol.Point) error {
	values := self.values[series]
	if values == nil {
		values = make(map[interface{}]float64)
		self.values[series] = values
	}
	currentValue, ok := values[group]
	if !ok {
		currentValue = self.initialValue
	}
	value, err := GetValue(self.value, self.columns, p)
	if err != nil {
		return err
	}
	values[group] = self.operation(currentValue, value)
	return nil
}

//TODO: to be optimized
func (self *CumulativeArithmeticAggregator) AggregateSeries(series string, group interface{}, s *protocol.Series) error {
	for _, p := range s.Points {
		self.AggregatePoint(series, group, p)
	}
	return nil
}

func (self *CumulativeArithmeticAggregator) ColumnNames() []string {
	return []string{self.name}
}

func (self *CumulativeArithmeticAggregator) GetValues(series string, group interface{}) [][]*protocol.FieldValue {
	returnValues := [][]*protocol.FieldValue{}
	value := self.values[series][group]
	defer delete(self.values[series], group)
	returnValues = append(returnValues, []*protocol.FieldValue{&protocol.FieldValue{DoubleValue: &value}})
	return returnValues
}

func NewCumulativeArithmeticAggregator(name string, value *parser.Value, initialValue float64, defaultValue *parser.Value, operation Operation) (Aggregator, error) {
	if len(value.Elems) != 1 {
		return nil, common.NewQueryError(common.WrongNumberOfArguments, "function max() requires only one argument")
	}

	wrappedDefaultValue, err := wrapDefaultValue(defaultValue)
	if err != nil {
		return nil, err
	}

	if value.Alias != "" {
		name = value.Alias
	}

	return &CumulativeArithmeticAggregator{
		AbstractAggregator: AbstractAggregator{
			value: value.Elems[0],
		},
		name:         name,
		values:       make(map[string]map[interface{}]float64),
		operation:    operation,
		initialValue: initialValue,
		defaultValue: wrappedDefaultValue,
	}, nil
}

func NewMaxAggregator(_ *parser.SelectQuery, value *parser.Value, defaultValue *parser.Value) (Aggregator, error) {
	return NewCumulativeArithmeticAggregator("max", value, -math.MaxFloat64, defaultValue, func(currentValue float64, p *protocol.FieldValue) float64 {
		if p.Int64Value != nil {
			if fv := float64(*p.Int64Value); fv > currentValue {
				return fv
			}
		} else if p.DoubleValue != nil {
			if fv := *p.DoubleValue; fv > currentValue {
				return fv
			}
		}
		return currentValue
	})
}

func NewMinAggregator(_ *parser.SelectQuery, value *parser.Value, defaultValue *parser.Value) (Aggregator, error) {
	return NewCumulativeArithmeticAggregator("min", value, math.MaxFloat64, defaultValue, func(currentValue float64, p *protocol.FieldValue) float64 {
		if p.Int64Value != nil {
			if fv := float64(*p.Int64Value); fv < currentValue {
				return fv
			}
		} else if p.DoubleValue != nil {
			if fv := *p.DoubleValue; fv < currentValue {
				return fv
			}
		}
		return currentValue
	})
}

func NewSumAggregator(_ *parser.SelectQuery, value *parser.Value, defaultValue *parser.Value) (Aggregator, error) {
	return NewCumulativeArithmeticAggregator("sum", value, 0, defaultValue, func(currentValue float64, p *protocol.FieldValue) float64 {
		var fv float64
		if p.Int64Value != nil {
			fv = float64(*p.Int64Value)
		} else if p.DoubleValue != nil {
			fv = *p.DoubleValue
		}
		return currentValue + fv
	})
}

type FirstOrLastAggregator struct {
	AbstractAggregator
	name         string
	isFirst      bool
	values       map[string]map[interface{}]*protocol.FieldValue
	defaultValue *protocol.FieldValue
}

func (self *FirstOrLastAggregator) AggregatePoint(series string, group interface{}, p *protocol.Point) error {
	values := self.values[series]
	if values == nil {
		values = make(map[interface{}]*protocol.FieldValue)
		self.values[series] = values
	}
	if values[group] == nil || !self.isFirst {
		value, err := GetValue(self.value, self.columns, p)
		if err != nil {
			return err
		}

		values[group] = value
	}
	return nil
}

//TODO: to be optimized
func (self *FirstOrLastAggregator) AggregateSeries(series string, group interface{}, s *protocol.Series) error {
	for _, p := range s.Points {
		self.AggregatePoint(series, group, p)
	}
	return nil
}

func (self *FirstOrLastAggregator) ColumnNames() []string {
	return []string{self.name}
}

func (self *FirstOrLastAggregator) GetValues(series string, group interface{}) [][]*protocol.FieldValue {
	defer delete(self.values[series], group)
	return [][]*protocol.FieldValue{
		[]*protocol.FieldValue{
			self.values[series][group],
		},
	}
}

func NewFirstOrLastAggregator(name string, v *parser.Value, isFirst bool, defaultValue *parser.Value) (Aggregator, error) {
	if len(v.Elems) != 1 {
		return nil, common.NewQueryError(common.WrongNumberOfArguments, "function max() requires only one argument")
	}

	wrappedDefaultValue, err := wrapDefaultValue(defaultValue)
	if err != nil {
		return nil, err
	}

	if v.Alias != "" {
		name = v.Alias
	}

	return &FirstOrLastAggregator{
		AbstractAggregator: AbstractAggregator{
			value: v.Elems[0],
		},
		name:         name,
		isFirst:      isFirst,
		values:       make(map[string]map[interface{}]*protocol.FieldValue),
		defaultValue: wrappedDefaultValue,
	}, nil
}

func NewFirstAggregator(_ *parser.SelectQuery, value *parser.Value, defaultValue *parser.Value) (Aggregator, error) {
	return NewFirstOrLastAggregator("first", value, true, defaultValue)
}

func NewLastAggregator(_ *parser.SelectQuery, value *parser.Value, defaultValue *parser.Value) (Aggregator, error) {
	return NewFirstOrLastAggregator("last", value, false, defaultValue)
}
