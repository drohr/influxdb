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
	AggregatePoint(state interface{}, p *protocol.Point) (interface{}, error)
	InitializeFieldsMetadata(series *protocol.Series) error
	GetValues(state interface{}) [][]*protocol.FieldValue
	CalculateSummaries(state interface{})
	ColumnNames() []string
}

// Initialize a new aggregator given the query, the function call of
// the aggregator and the default value that should be returned if
// the bucket doesn't have any points
type AggregatorInitializer func(*parser.SelectQuery, *parser.Value, *parser.Value) (Aggregator, error)

var registeredAggregators = make(map[string]AggregatorInitializer)

func init() {
	registeredAggregators["max"] = NewMaxAggregator
	registeredAggregators["count"] = NewCountAggregator
	registeredAggregators["histogram"] = NewHistogramAggregator
	registeredAggregators["derivative"] = NewDerivativeAggregator
	registeredAggregators["stddev"] = NewStandardDeviationAggregator
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

func (self *AbstractAggregator) CalculateSummaries(state interface{}) {
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

type Operation func(currentValue float64, newValue *protocol.FieldValue) float64

type CumulativeArithmeticAggregatorState float64

type CumulativeArithmeticAggregator struct {
	AbstractAggregator
	name         string
	operation    Operation
	initialValue float64
	defaultValue *protocol.FieldValue
}

var count int = 0

func (self *CumulativeArithmeticAggregator) AggregatePoint(state interface{}, p *protocol.Point) (interface{}, error) {
	if state == nil {
		state = self.initialValue
	}

	value, err := GetValue(self.value, self.columns, p)
	if err != nil {
		return nil, err
	}
	state = self.operation(state.(float64), value)
	count++
	if count%100000 == 0 {
		fmt.Printf("new state: %f\n", state)
	}
	return state, nil
}

func (self *CumulativeArithmeticAggregator) ColumnNames() []string {
	return []string{self.name}
}

func (self *CumulativeArithmeticAggregator) GetValues(state interface{}) [][]*protocol.FieldValue {
	if state == nil {
		return [][]*protocol.FieldValue{
			[]*protocol.FieldValue{
				self.defaultValue,
			},
		}
	}

	return [][]*protocol.FieldValue{
		[]*protocol.FieldValue{
			&protocol.FieldValue{
				DoubleValue: protocol.Float64(state.(float64)),
			},
		},
	}
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

//
// Composite Aggregator
//

type CompositeAggregator struct {
	left  Aggregator
	right Aggregator
}

func (self *CompositeAggregator) AggregatePoint(state interface{}, p *protocol.Point) (interface{}, error) {
	return self.right.AggregatePoint(state, p)
}

func (self *CompositeAggregator) ColumnNames() []string {
	return self.left.ColumnNames()
}

func (self *CompositeAggregator) CalculateSummaries(state interface{}) {
	self.right.CalculateSummaries(state)
	values := self.right.GetValues(state)
	for _, v := range values {
		point := &protocol.Point{Values: v}
		self.left.AggregatePoint(state, point)
	}
	self.left.CalculateSummaries(state)
}

func (self *CompositeAggregator) GetValues(state interface{}) [][]*protocol.FieldValue {
	return self.left.GetValues(state)
}

func (self *CompositeAggregator) InitializeFieldsMetadata(series *protocol.Series) error {
	return self.right.InitializeFieldsMetadata(series)
}

func NewCompositeAggregator(left, right Aggregator) (Aggregator, error) {
	return &CompositeAggregator{left, right}, nil
}

// StandardDeviation Aggregator

type StandardDeviationRunning struct {
	count   int
	totalX2 float64
	totalX  float64
}

type StandardDeviationAggregator struct {
	AbstractAggregator
	defaultValue *protocol.FieldValue
	alias        string
}

func (self *StandardDeviationAggregator) AggregatePoint(state interface{}, p *protocol.Point) (interface{}, error) {
	fieldValue, err := GetValue(self.value, self.columns, p)
	if err != nil {
		return nil, err
	}

	var value float64
	if ptr := fieldValue.Int64Value; ptr != nil {
		value = float64(*ptr)
	} else if ptr := fieldValue.DoubleValue; ptr != nil {
		value = *ptr
	} else {
		// else ignore this point
		return state, nil
	}

	running, ok := state.(*StandardDeviationRunning)
	if !ok {
		running = &StandardDeviationRunning{}
	}

	running.count++
	running.totalX += value
	running.totalX2 += value * value
	return running, nil
}

func (self *StandardDeviationAggregator) ColumnNames() []string {
	if self.alias != "" {
		return []string{self.alias}
	}

	return []string{"stddev"}
}

func (self *StandardDeviationAggregator) GetValues(state interface{}) [][]*protocol.FieldValue {
	r, ok := state.(*StandardDeviationRunning)
	if !ok {
		return nil
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
		defaultValue: value,
		alias:        v.Alias,
	}, nil
}

//
// Derivative Aggregator
//

type DerivativeAggregatorState struct {
	firstValue *protocol.Point
	lastValue  *protocol.Point
}

type DerivativeAggregator struct {
	AbstractAggregator
	defaultValue *protocol.FieldValue
	alias        string
}

func (self *DerivativeAggregator) AggregatePoint(state interface{}, p *protocol.Point) (interface{}, error) {
	fieldValue, err := GetValue(self.value, self.columns, p)
	if err != nil {
		return nil, err
	}

	var value float64
	if ptr := fieldValue.Int64Value; ptr != nil {
		value = float64(*ptr)
	} else if ptr := fieldValue.DoubleValue; ptr != nil {
		value = *ptr
	} else {
		// else ignore this point
		return state, nil
	}

	newValue := &protocol.Point{
		Timestamp: p.Timestamp,
		Values:    []*protocol.FieldValue{&protocol.FieldValue{DoubleValue: &value}},
	}

	s, ok := state.(*DerivativeAggregatorState)
	if !ok {
		s = &DerivativeAggregatorState{}
	}

	if s.firstValue == nil {
		s.firstValue = newValue
		return s, nil
	}

	s.lastValue = newValue
	return s, nil
}

func (self *DerivativeAggregator) ColumnNames() []string {
	if self.alias != "" {
		return []string{self.alias}
	}
	return []string{"derivative"}
}

func (self *DerivativeAggregator) GetValues(state interface{}) [][]*protocol.FieldValue {
	s, ok := state.(*DerivativeAggregatorState)

	if !ok && s.firstValue != nil && s.lastValue != nil {
		// if an old value exist, then compute the derivative and insert it in the points slice
		deltaT := float64(*s.lastValue.Timestamp-*s.firstValue.Timestamp) / float64(time.Second/time.Microsecond)
		deltaV := *s.lastValue.Values[0].DoubleValue - *s.lastValue.Values[0].DoubleValue
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
		defaultValue: wrappedDefaultValue,
		alias:        v.Alias,
	}, nil
}

//
// Histogram Aggregator
//

type HistogramAggregatorState map[int]int

type HistogramAggregator struct {
	AbstractAggregator
	bucketSize  float64
	columnNames []string
}

func (self *HistogramAggregator) AggregatePoint(state interface{}, p *protocol.Point) (interface{}, error) {
	buckets := state.(HistogramAggregatorState)
	if buckets == nil {
		buckets = make(map[int]int)
	}

	fieldValue, err := GetValue(self.value, self.columns, p)
	if err != nil {
		return nil, err
	}

	var value float64
	if ptr := fieldValue.Int64Value; ptr != nil {
		value = float64(*ptr)
	} else if ptr := fieldValue.DoubleValue; ptr != nil {
		value = *ptr
	}

	bucket := int(value / self.bucketSize)
	buckets[bucket] += 1

	return buckets, nil
}

func (self *HistogramAggregator) ColumnNames() []string {
	return self.columnNames
}

func (self *HistogramAggregator) GetValues(state interface{}) [][]*protocol.FieldValue {
	returnValues := [][]*protocol.FieldValue{}
	buckets := state.(HistogramAggregatorState)
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
		columnNames: columnNames,
	}, nil
}

//
// Count Aggregator
//

type CountAggregator struct {
	AbstractAggregator
	defaultValue *protocol.FieldValue
	alias        string
}

type CountAggregatorState int64

func (self *CountAggregator) AggregatePoint(state interface{}, p *protocol.Point) (interface{}, error) {
	return state.(int64) + 1, nil
}

func (self *CountAggregator) ColumnNames() []string {
	if self.alias != "" {
		return []string{self.alias}
	}
	return []string{"count"}
}

func (self *CountAggregator) GetValues(state interface{}) [][]*protocol.FieldValue {
	returnValues := [][]*protocol.FieldValue{}
	if state == nil {
		returnValues = append(returnValues, []*protocol.FieldValue{self.defaultValue})
	} else {
		value := state.(int64)
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
		return NewCompositeAggregator(&CountAggregator{AbstractAggregator{}, wrappedDefaultValue, v.Alias}, inner)
	}

	return &CountAggregator{AbstractAggregator{}, wrappedDefaultValue, v.Alias}, nil
}

//
// Mean Aggregator
//

type MeanAggregatorState struct {
	mean  float64
	count int
}

type MeanAggregator struct {
	AbstractAggregator
	defaultValue *protocol.FieldValue
	alias        string
}

func (self *MeanAggregator) AggregatePoint(state interface{}, p *protocol.Point) (interface{}, error) {
	s, ok := state.(*MeanAggregatorState)
	if !ok {
		s = &MeanAggregatorState{}
	}

	currentMean := s.mean
	currentCount := s.count + 1

	fieldValue, err := GetValue(self.value, self.columns, p)
	if err != nil {
		return nil, err
	}

	var value float64
	if ptr := fieldValue.Int64Value; ptr != nil {
		value = float64(*ptr)
	} else if ptr := fieldValue.DoubleValue; ptr != nil {
		value = *ptr
	}

	s.mean = currentMean*float64(currentCount-1)/float64(currentCount) + value/float64(currentCount)

	return s, nil
}

func (self *MeanAggregator) ColumnNames() []string {
	if self.alias != "" {
		return []string{self.alias}
	}
	return []string{"mean"}
}

func (self *MeanAggregator) GetValues(state interface{}) [][]*protocol.FieldValue {
	returnValues := [][]*protocol.FieldValue{}
	s, ok := state.(*MeanAggregatorState)
	if !ok {
		returnValues = append(returnValues, []*protocol.FieldValue{self.defaultValue})
	} else {
		returnValues = append(returnValues, []*protocol.FieldValue{
			&protocol.FieldValue{DoubleValue: &s.mean},
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
		defaultValue: wrappedDefaultValue,
		alias:        value.Alias,
	}
	return aggregator, nil
}

//
// Percentile Aggregator
//

type PercentileAggregatorState struct {
	values          []float64
	percentileValue float64
}

type PercentileAggregator struct {
	AbstractAggregator
	functionName string
	percentile   float64
	defaultValue *protocol.FieldValue
	alias        string
}

func (self *PercentileAggregator) AggregatePoint(state interface{}, p *protocol.Point) (interface{}, error) {
	v, err := GetValue(self.value, self.columns, p)
	if err != nil {
		return nil, err
	}

	value := 0.0
	if v.Int64Value != nil {
		value = float64(*v.Int64Value)
	} else if v.DoubleValue != nil {
		value = *v.DoubleValue
	} else {
		return state, nil
	}

	s, ok := state.(*PercentileAggregatorState)
	if !ok {
		s = &PercentileAggregatorState{}
	}

	s.values = append(s.values, value)

	return s, nil
}

func (self *PercentileAggregator) ColumnNames() []string {
	if self.alias != "" {
		return []string{self.alias}
	}
	return []string{self.functionName}
}

func (self *PercentileAggregator) GetValues(state interface{}) [][]*protocol.FieldValue {
	s, ok := state.(*PercentileAggregatorState)
	if !ok {
		return [][]*protocol.FieldValue{
			[]*protocol.FieldValue{self.defaultValue},
		}
	}
	return [][]*protocol.FieldValue{
		[]*protocol.FieldValue{&protocol.FieldValue{DoubleValue: &s.percentileValue}},
	}
}

func (self *PercentileAggregator) CalculateSummaries(state interface{}) {
	s := state.(*PercentileAggregatorState)
	sort.Float64s(s.values)
	length := len(s.values)
	index := int(math.Floor(float64(length)*self.percentile/100.0+0.5)) - 1

	if index < 0 || index >= len(s.values) {
		return
	}

	s.percentileValue = s.values[index]
	s.values = nil
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
		defaultValue: wrappedDefaultValue,
	}, nil
}

//
// Mode Aggregator
//

type ModeAggregatorState struct {
	counts map[float64]int
	modes  []float64
}

type ModeAggregator struct {
	AbstractAggregator
	defaultValue *protocol.FieldValue
	alias        string
}

func (self *ModeAggregator) AggregatePoint(state interface{}, p *protocol.Point) (interface{}, error) {
	s, ok := state.(*ModeAggregatorState)
	if !ok {
		s = &ModeAggregatorState{}
	}

	point, err := GetValue(self.value, self.columns, p)
	if err != nil {
		return nil, err
	}

	var value float64
	if point.Int64Value != nil {
		value = float64(*point.Int64Value)
	} else if point.DoubleValue != nil {
		value = *point.DoubleValue
	} else {
		return s, nil
	}

	s.counts[value]++
	return s, nil
}

func (self *ModeAggregator) ColumnNames() []string {
	if self.alias != "" {
		return []string{self.alias}
	}
	return []string{"mode"}
}

func (self *ModeAggregator) CalculateSummaries(state interface{}) {
	modes := []float64{}
	currentCount := 1

	s := state.(*ModeAggregatorState)
	for value, count := range s.counts {
		if count == currentCount {
			modes = append(modes, value)
		} else if count > currentCount {
			modes = nil
			modes = append(modes, value)
			currentCount = count
		}
	}

	s.modes = modes
	s.counts = nil
}

func (self *ModeAggregator) GetValues(state interface{}) [][]*protocol.FieldValue {
	returnValues := [][]*protocol.FieldValue{}

	s, ok := state.(*ModeAggregatorState)
	if !ok || len(s.modes) == 0 {
		returnValues = append(returnValues, []*protocol.FieldValue{self.defaultValue})
		return returnValues
	}

	for _, value := range s.modes {
		// we can't use value since we need a pointer to a variable that won't change,
		// while value will change the next iteration
		v := value
		returnValues = append(returnValues, []*protocol.FieldValue{
			&protocol.FieldValue{DoubleValue: &v},
		})
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
		defaultValue: wrappedDefaultValue,
		alias:        value.Alias,
	}, nil
}

//
// Distinct Aggregator
//

type DistinctAggregatorState struct {
	counts map[interface{}]int
}

type DistinctAggregator struct {
	AbstractAggregator
	defaultValue *protocol.FieldValue
	alias        string
}

func (self *DistinctAggregator) AggregatePoint(state interface{}, p *protocol.Point) (interface{}, error) {
	s, ok := state.(*DistinctAggregatorState)
	if !ok {
		s = &DistinctAggregatorState{}
	}

	point, err := GetValue(self.value, self.columns, p)
	if err != nil {
		return nil, err
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
		value = nil
	}

	s.counts[value]++

	return s, nil
}

func (self *DistinctAggregator) ColumnNames() []string {
	if self.alias != "" {
		return []string{self.alias}
	}
	return []string{"distinct"}
}

func (self *DistinctAggregator) GetValues(state interface{}) [][]*protocol.FieldValue {
	returnValues := [][]*protocol.FieldValue{}
	s, ok := state.(*DistinctAggregatorState)
	if !ok || len(s.counts) == 0 {
		returnValues = append(returnValues, []*protocol.FieldValue{self.defaultValue})
	}

	for value, _ := range s.counts {
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
		defaultValue: wrappedDefaultValue,
		alias:        value.Alias,
	}, nil
}

//
// Max, Min and Sum Aggregators
//

type FirstOrLastAggregatorState *protocol.FieldValue

type FirstOrLastAggregator struct {
	AbstractAggregator
	name         string
	isFirst      bool
	defaultValue *protocol.FieldValue
}

func (self *FirstOrLastAggregator) AggregatePoint(state interface{}, p *protocol.Point) (interface{}, error) {
	s := state.(FirstOrLastAggregatorState)
	if s == nil || !self.isFirst {
		value, err := GetValue(self.value, self.columns, p)
		if err != nil {
			return nil, err
		}

		s = value
	}
	return s, nil
}

func (self *FirstOrLastAggregator) ColumnNames() []string {
	return []string{self.name}
}

func (self *FirstOrLastAggregator) GetValues(state interface{}) [][]*protocol.FieldValue {
	s := state.(FirstOrLastAggregatorState)
	return [][]*protocol.FieldValue{
		[]*protocol.FieldValue{
			s,
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
		defaultValue: wrappedDefaultValue,
	}, nil
}

func NewFirstAggregator(_ *parser.SelectQuery, value *parser.Value, defaultValue *parser.Value) (Aggregator, error) {
	return NewFirstOrLastAggregator("first", value, true, defaultValue)
}

func NewLastAggregator(_ *parser.SelectQuery, value *parser.Value, defaultValue *parser.Value) (Aggregator, error) {
	return NewFirstOrLastAggregator("last", value, false, defaultValue)
}
