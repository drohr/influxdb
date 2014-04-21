package engine

import (
	"sort"
)

type SortableGroups interface {
	sort.Interface
	GetSortedGroups() []Group
}

type CommonSortableGroups struct {
	data  []Group
	table string
}

func (self *CommonSortableGroups) GetSortedGroups() []Group {
	return self.data
}

type AscendingAggregatorSortableGroups struct {
	CommonSortableGroups
	aggregator *TimestampAggregator
}

func (self *CommonSortableGroups) Len() int {
	return len(self.data)
}

func (self *CommonSortableGroups) Swap(i, j int) {
	self.data[i], self.data[j] = self.data[j], self.data[i]
}

func (self *AscendingAggregatorSortableGroups) Less(i, j int) bool {
	iTimestamp := self.aggregator.GetValuesOptionalDelete(self.table, self.data[i], false)[0][0].Int64Value
	jTimestamp := self.aggregator.GetValuesOptionalDelete(self.table, self.data[j], false)[0][0].Int64Value
	return *iTimestamp < *jTimestamp
}

type AscendingGroupTimestampSortableGroups struct {
	CommonSortableGroups
}

func (self *AscendingGroupTimestampSortableGroups) Less(i, j int) bool {
	return self.data[i].GetTimestamp() < self.data[j].GetTimestamp()
}
