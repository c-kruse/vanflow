package vanflow

import (
	"github.com/c-kruse/vanflow/encoding"
)

const apiVersion = "flow/v1"

func init() {
	encoding.MustRegisterRecord(0, SiteRecord{})
	encoding.MustRegisterRecord(1, RouterRecord{})
	encoding.MustRegisterRecord(2, LinkRecord{})
	encoding.MustRegisterRecord(3, ControllerRecord{})
	encoding.MustRegisterRecord(4, ListenerRecord{})
	encoding.MustRegisterRecord(5, ConnectorRecord{})
	encoding.MustRegisterRecord(6, FlowRecord{})
	encoding.MustRegisterRecord(7, ProcessRecord{})
	encoding.MustRegisterRecord(8, ImageRecord{})
	encoding.MustRegisterRecord(9, IngressRecord{})
	encoding.MustRegisterRecord(10, EgressRecord{})
	encoding.MustRegisterRecord(11, CollectorRecord{})
	encoding.MustRegisterRecord(12, ProcessGroupRecord{})
	encoding.MustRegisterRecord(13, HostRecord{})
	encoding.MustRegisterRecord(14, LogRecord{})
}

type SiteRecord struct {
	BaseRecord
	Location  *string `vflow:"9"`
	Provider  *string `vflow:"10"`
	Platform  *string `vflow:"11"`
	Namespace *string `vflow:"12"`
	Name      *string `vflow:"30"` //unspeced
	Version   *string `vflow:"52"`
	Policy    *string `vflow:"53"`
}

func (r SiteRecord) GetTypeMeta() TypeMeta {
	return TypeMeta{
		APIVersion: apiVersion,
		Type:       "SiteRecord",
	}
}

type RouterRecord struct {
	BaseRecord
	Parent       *string `vflow:"2"`
	Namespace    *string `vflow:"12"`
	Mode         *string `vflow:"13"`
	ImageName    *string `vflow:"20"`
	ImageVersion *string `vflow:"21"`
	Hostname     *string `vflow:"22"`
	Name         *string `vflow:"30"`
	BuildVersion *string `vflow:"32"`
}

func (r RouterRecord) GetTypeMeta() TypeMeta {
	return TypeMeta{
		APIVersion: apiVersion,
		Type:       "RouterRecord",
	}
}

type LinkRecord struct {
	BaseRecord
	Parent    *string `vflow:"2"`
	Mode      *string `vflow:"13"`
	Name      *string `vflow:"30"`
	LinkCost  *uint64 `vflow:"33"`
	Direction *string `vflow:"34"`
}

func (r LinkRecord) GetTypeMeta() TypeMeta {
	return TypeMeta{
		APIVersion: apiVersion,
		Type:       "LinkRecord",
	}
}

type ControllerRecord struct {
	BaseRecord
	Parent       *string `vflow:"2"`
	ImageName    *string `vflow:"20"`
	ImageVersion *string `vflow:"21"`
	Hostname     *string `vflow:"22"`
	Name         *string `vflow:"30"`
	BuildVersion *string `vflow:"32"`
}

func (r ControllerRecord) GetTypeMeta() TypeMeta {
	return TypeMeta{
		APIVersion: apiVersion,
		Type:       "ControllerRecord",
	}
}

type ListenerRecord struct {
	BaseRecord
	Parent      *string `vflow:"2"`
	DestHost    *string `vflow:"15"`
	Protocol    *string `vflow:"16"`
	DestPort    *string `vflow:"18"`
	Address     *string `vflow:"19"`
	FlowCountL4 *uint64 `vflow:"40"`
	FlowCountL7 *uint64 `vflow:"41"`
	FlowRateL4  *uint64 `vflow:"42"`
	FlowRateL7  *uint64 `vflow:"43"`
}

func (r ListenerRecord) GetTypeMeta() TypeMeta {
	return TypeMeta{
		APIVersion: apiVersion,
		Type:       "ListenerRecord",
	}
}

type ConnectorRecord struct {
	BaseRecord
	Parent      *string `vflow:"2"`
	DestHost    *string `vflow:"15"`
	Protocol    *string `vflow:"16"`
	DestPort    *string `vflow:"18"`
	Address     *string `vflow:"19"`
	FlowCountL4 *uint64 `vflow:"40"`
	FlowCountL7 *uint64 `vflow:"41"`
	FlowRateL4  *uint64 `vflow:"42"`
	FlowRateL7  *uint64 `vflow:"43"`
}

func (r ConnectorRecord) GetTypeMeta() TypeMeta {
	return TypeMeta{
		APIVersion: apiVersion,
		Type:       "ConnectorRecord",
	}
}

type FlowRecord struct {
	BaseRecord
	Parent         *string `vflow:"2"`
	Counterflow    *string `vflow:"5"`
	SourceHost     *string `vflow:"14"`
	SourcePort     *string `vflow:"17"`
	Octets         *uint64 `vflow:"23"`
	Latency        *uint64 `vflow:"24"`
	Reason         *string `vflow:"29"`
	Trace          *string `vflow:"31"`
	OctetRate      *uint64 `vflow:"35"`
	OctetsOut      *uint64 `vflow:"36"`
	OctetsUnacked  *uint64 `vflow:"37"`
	WindowClosures *uint64 `vflow:"38"`
	WindowSize     *uint64 `vflow:"39"`
	Method         *string `vflow:"27"`
	Result         *string `vflow:"28"`
}

func (r FlowRecord) GetTypeMeta() TypeMeta {
	return TypeMeta{
		APIVersion: apiVersion,
		Type:       "FlowRecord",
	}
}

type ProcessRecord struct {
	BaseRecord
	Parent       *string `vflow:"2"` //unspeced
	Mode         *string `vflow:"13"`
	SourceHost   *string `vflow:"14"`
	ImageName    *string `vflow:"20"`
	ImageVersion *string `vflow:"21"`
	Hostname     *string `vflow:"22"`
	Name         *string `vflow:"30"`
	Group        *string `vflow:"46"`
}

func (r ProcessRecord) GetTypeMeta() TypeMeta {
	return TypeMeta{
		APIVersion: apiVersion,
		Type:       "ProcessRecord",
	}
}

type ImageRecord struct {
	BaseRecord
}

type IngressRecord struct {
	BaseRecord
}

type EgressRecord struct {
	BaseRecord
}

type CollectorRecord struct {
	BaseRecord
}

type ProcessGroupRecord struct {
	BaseRecord
}

type HostRecord struct {
	BaseRecord
	Provider *string `vflow:"10"`
	Name     *string `vflow:"30"`
}

func (r HostRecord) GetTypeMeta() TypeMeta {
	return TypeMeta{
		APIVersion: apiVersion,
		Type:       "HostRecord",
	}
}

type LogRecord struct {
	BaseRecord
	LogSeverity *uint64 `vflow:"48"`
	LogText     *string `vflow:"49"`
	SourceFile  *string `vflow:"50"`
	SourceLine  *uint64 `vflow:"51"`
}

func (r LogRecord) GetTypeMeta() TypeMeta {
	return TypeMeta{
		APIVersion: apiVersion,
		Type:       "LogRecord",
	}
}
