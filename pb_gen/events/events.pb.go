// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.27.1
// 	protoc        v3.19.4
// source: events.proto

package events

import (
	configs "UNS/pb_gen/configs"
	objects "UNS/pb_gen/objects"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type RMRegisterResourceManagerEvent struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	ResourceManagerID string                   `protobuf:"bytes,1,opt,name=resourceManagerID,proto3" json:"resourceManagerID,omitempty"`
	Configuration     *configs.RMConfiguration `protobuf:"bytes,2,opt,name=configuration,proto3" json:"configuration,omitempty"`
}

func (x *RMRegisterResourceManagerEvent) Reset() {
	*x = RMRegisterResourceManagerEvent{}
	if protoimpl.UnsafeEnabled {
		mi := &file_events_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *RMRegisterResourceManagerEvent) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*RMRegisterResourceManagerEvent) ProtoMessage() {}

func (x *RMRegisterResourceManagerEvent) ProtoReflect() protoreflect.Message {
	mi := &file_events_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use RMRegisterResourceManagerEvent.ProtoReflect.Descriptor instead.
func (*RMRegisterResourceManagerEvent) Descriptor() ([]byte, []int) {
	return file_events_proto_rawDescGZIP(), []int{0}
}

func (x *RMRegisterResourceManagerEvent) GetResourceManagerID() string {
	if x != nil {
		return x.ResourceManagerID
	}
	return ""
}

func (x *RMRegisterResourceManagerEvent) GetConfiguration() *configs.RMConfiguration {
	if x != nil {
		return x.Configuration
	}
	return nil
}

type RMUpdateAllocationsEvent struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	JobAllocations  []*objects.JobAllocation `protobuf:"bytes,1,rep,name=jobAllocations,proto3" json:"jobAllocations,omitempty"`
	CurrentNanoTime int64                    `protobuf:"varint,255,opt,name=currentNanoTime,proto3" json:"currentNanoTime,omitempty"`
}

func (x *RMUpdateAllocationsEvent) Reset() {
	*x = RMUpdateAllocationsEvent{}
	if protoimpl.UnsafeEnabled {
		mi := &file_events_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *RMUpdateAllocationsEvent) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*RMUpdateAllocationsEvent) ProtoMessage() {}

func (x *RMUpdateAllocationsEvent) ProtoReflect() protoreflect.Message {
	mi := &file_events_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use RMUpdateAllocationsEvent.ProtoReflect.Descriptor instead.
func (*RMUpdateAllocationsEvent) Descriptor() ([]byte, []int) {
	return file_events_proto_rawDescGZIP(), []int{1}
}

func (x *RMUpdateAllocationsEvent) GetJobAllocations() []*objects.JobAllocation {
	if x != nil {
		return x.JobAllocations
	}
	return nil
}

func (x *RMUpdateAllocationsEvent) GetCurrentNanoTime() int64 {
	if x != nil {
		return x.CurrentNanoTime
	}
	return 0
}

type RMUpdateJobsEvent struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	NewJobs         []*objects.Job `protobuf:"bytes,1,rep,name=newJobs,proto3" json:"newJobs,omitempty"`
	RemovedJobIDs   []string       `protobuf:"bytes,2,rep,name=removedJobIDs,proto3" json:"removedJobIDs,omitempty"`
	CurrentNanoTime int64          `protobuf:"varint,255,opt,name=currentNanoTime,proto3" json:"currentNanoTime,omitempty"`
}

func (x *RMUpdateJobsEvent) Reset() {
	*x = RMUpdateJobsEvent{}
	if protoimpl.UnsafeEnabled {
		mi := &file_events_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *RMUpdateJobsEvent) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*RMUpdateJobsEvent) ProtoMessage() {}

func (x *RMUpdateJobsEvent) ProtoReflect() protoreflect.Message {
	mi := &file_events_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use RMUpdateJobsEvent.ProtoReflect.Descriptor instead.
func (*RMUpdateJobsEvent) Descriptor() ([]byte, []int) {
	return file_events_proto_rawDescGZIP(), []int{2}
}

func (x *RMUpdateJobsEvent) GetNewJobs() []*objects.Job {
	if x != nil {
		return x.NewJobs
	}
	return nil
}

func (x *RMUpdateJobsEvent) GetRemovedJobIDs() []string {
	if x != nil {
		return x.RemovedJobIDs
	}
	return nil
}

func (x *RMUpdateJobsEvent) GetCurrentNanoTime() int64 {
	if x != nil {
		return x.CurrentNanoTime
	}
	return 0
}

type RMUpdateTimeEvent struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	CurrentNanoTime int64 `protobuf:"varint,255,opt,name=currentNanoTime,proto3" json:"currentNanoTime,omitempty"`
}

func (x *RMUpdateTimeEvent) Reset() {
	*x = RMUpdateTimeEvent{}
	if protoimpl.UnsafeEnabled {
		mi := &file_events_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *RMUpdateTimeEvent) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*RMUpdateTimeEvent) ProtoMessage() {}

func (x *RMUpdateTimeEvent) ProtoReflect() protoreflect.Message {
	mi := &file_events_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use RMUpdateTimeEvent.ProtoReflect.Descriptor instead.
func (*RMUpdateTimeEvent) Descriptor() ([]byte, []int) {
	return file_events_proto_rawDescGZIP(), []int{3}
}

func (x *RMUpdateTimeEvent) GetCurrentNanoTime() int64 {
	if x != nil {
		return x.CurrentNanoTime
	}
	return 0
}

type SSUpdateAllocationsEvent struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	NewJobAllocations []*objects.JobAllocation `protobuf:"bytes,1,rep,name=newJobAllocations,proto3" json:"newJobAllocations,omitempty"`
}

func (x *SSUpdateAllocationsEvent) Reset() {
	*x = SSUpdateAllocationsEvent{}
	if protoimpl.UnsafeEnabled {
		mi := &file_events_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *SSUpdateAllocationsEvent) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SSUpdateAllocationsEvent) ProtoMessage() {}

func (x *SSUpdateAllocationsEvent) ProtoReflect() protoreflect.Message {
	mi := &file_events_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use SSUpdateAllocationsEvent.ProtoReflect.Descriptor instead.
func (*SSUpdateAllocationsEvent) Descriptor() ([]byte, []int) {
	return file_events_proto_rawDescGZIP(), []int{4}
}

func (x *SSUpdateAllocationsEvent) GetNewJobAllocations() []*objects.JobAllocation {
	if x != nil {
		return x.NewJobAllocations
	}
	return nil
}

var File_events_proto protoreflect.FileDescriptor

var file_events_proto_rawDesc = []byte{
	0x0a, 0x0c, 0x65, 0x76, 0x65, 0x6e, 0x74, 0x73, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x06,
	0x70, 0x62, 0x5f, 0x67, 0x65, 0x6e, 0x1a, 0x0d, 0x6f, 0x62, 0x6a, 0x65, 0x63, 0x74, 0x73, 0x2e,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x0c, 0x63, 0x6f, 0x6e, 0x66, 0x69, 0x67, 0x2e, 0x70, 0x72,
	0x6f, 0x74, 0x6f, 0x22, 0x8d, 0x01, 0x0a, 0x1e, 0x52, 0x4d, 0x52, 0x65, 0x67, 0x69, 0x73, 0x74,
	0x65, 0x72, 0x52, 0x65, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x4d, 0x61, 0x6e, 0x61, 0x67, 0x65,
	0x72, 0x45, 0x76, 0x65, 0x6e, 0x74, 0x12, 0x2c, 0x0a, 0x11, 0x72, 0x65, 0x73, 0x6f, 0x75, 0x72,
	0x63, 0x65, 0x4d, 0x61, 0x6e, 0x61, 0x67, 0x65, 0x72, 0x49, 0x44, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x11, 0x72, 0x65, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x4d, 0x61, 0x6e, 0x61, 0x67,
	0x65, 0x72, 0x49, 0x44, 0x12, 0x3d, 0x0a, 0x0d, 0x63, 0x6f, 0x6e, 0x66, 0x69, 0x67, 0x75, 0x72,
	0x61, 0x74, 0x69, 0x6f, 0x6e, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x17, 0x2e, 0x70, 0x62,
	0x5f, 0x67, 0x65, 0x6e, 0x2e, 0x52, 0x4d, 0x43, 0x6f, 0x6e, 0x66, 0x69, 0x67, 0x75, 0x72, 0x61,
	0x74, 0x69, 0x6f, 0x6e, 0x52, 0x0d, 0x63, 0x6f, 0x6e, 0x66, 0x69, 0x67, 0x75, 0x72, 0x61, 0x74,
	0x69, 0x6f, 0x6e, 0x22, 0x84, 0x01, 0x0a, 0x18, 0x52, 0x4d, 0x55, 0x70, 0x64, 0x61, 0x74, 0x65,
	0x41, 0x6c, 0x6c, 0x6f, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x45, 0x76, 0x65, 0x6e, 0x74,
	0x12, 0x3d, 0x0a, 0x0e, 0x6a, 0x6f, 0x62, 0x41, 0x6c, 0x6c, 0x6f, 0x63, 0x61, 0x74, 0x69, 0x6f,
	0x6e, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x15, 0x2e, 0x70, 0x62, 0x5f, 0x67, 0x65,
	0x6e, 0x2e, 0x4a, 0x6f, 0x62, 0x41, 0x6c, 0x6c, 0x6f, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x52,
	0x0e, 0x6a, 0x6f, 0x62, 0x41, 0x6c, 0x6c, 0x6f, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x12,
	0x29, 0x0a, 0x0f, 0x63, 0x75, 0x72, 0x72, 0x65, 0x6e, 0x74, 0x4e, 0x61, 0x6e, 0x6f, 0x54, 0x69,
	0x6d, 0x65, 0x18, 0xff, 0x01, 0x20, 0x01, 0x28, 0x03, 0x52, 0x0f, 0x63, 0x75, 0x72, 0x72, 0x65,
	0x6e, 0x74, 0x4e, 0x61, 0x6e, 0x6f, 0x54, 0x69, 0x6d, 0x65, 0x22, 0x8b, 0x01, 0x0a, 0x11, 0x52,
	0x4d, 0x55, 0x70, 0x64, 0x61, 0x74, 0x65, 0x4a, 0x6f, 0x62, 0x73, 0x45, 0x76, 0x65, 0x6e, 0x74,
	0x12, 0x25, 0x0a, 0x07, 0x6e, 0x65, 0x77, 0x4a, 0x6f, 0x62, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28,
	0x0b, 0x32, 0x0b, 0x2e, 0x70, 0x62, 0x5f, 0x67, 0x65, 0x6e, 0x2e, 0x4a, 0x6f, 0x62, 0x52, 0x07,
	0x6e, 0x65, 0x77, 0x4a, 0x6f, 0x62, 0x73, 0x12, 0x24, 0x0a, 0x0d, 0x72, 0x65, 0x6d, 0x6f, 0x76,
	0x65, 0x64, 0x4a, 0x6f, 0x62, 0x49, 0x44, 0x73, 0x18, 0x02, 0x20, 0x03, 0x28, 0x09, 0x52, 0x0d,
	0x72, 0x65, 0x6d, 0x6f, 0x76, 0x65, 0x64, 0x4a, 0x6f, 0x62, 0x49, 0x44, 0x73, 0x12, 0x29, 0x0a,
	0x0f, 0x63, 0x75, 0x72, 0x72, 0x65, 0x6e, 0x74, 0x4e, 0x61, 0x6e, 0x6f, 0x54, 0x69, 0x6d, 0x65,
	0x18, 0xff, 0x01, 0x20, 0x01, 0x28, 0x03, 0x52, 0x0f, 0x63, 0x75, 0x72, 0x72, 0x65, 0x6e, 0x74,
	0x4e, 0x61, 0x6e, 0x6f, 0x54, 0x69, 0x6d, 0x65, 0x22, 0x3e, 0x0a, 0x11, 0x52, 0x4d, 0x55, 0x70,
	0x64, 0x61, 0x74, 0x65, 0x54, 0x69, 0x6d, 0x65, 0x45, 0x76, 0x65, 0x6e, 0x74, 0x12, 0x29, 0x0a,
	0x0f, 0x63, 0x75, 0x72, 0x72, 0x65, 0x6e, 0x74, 0x4e, 0x61, 0x6e, 0x6f, 0x54, 0x69, 0x6d, 0x65,
	0x18, 0xff, 0x01, 0x20, 0x01, 0x28, 0x03, 0x52, 0x0f, 0x63, 0x75, 0x72, 0x72, 0x65, 0x6e, 0x74,
	0x4e, 0x61, 0x6e, 0x6f, 0x54, 0x69, 0x6d, 0x65, 0x22, 0x5f, 0x0a, 0x18, 0x53, 0x53, 0x55, 0x70,
	0x64, 0x61, 0x74, 0x65, 0x41, 0x6c, 0x6c, 0x6f, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x45,
	0x76, 0x65, 0x6e, 0x74, 0x12, 0x43, 0x0a, 0x11, 0x6e, 0x65, 0x77, 0x4a, 0x6f, 0x62, 0x41, 0x6c,
	0x6c, 0x6f, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x0b, 0x32,
	0x15, 0x2e, 0x70, 0x62, 0x5f, 0x67, 0x65, 0x6e, 0x2e, 0x4a, 0x6f, 0x62, 0x41, 0x6c, 0x6c, 0x6f,
	0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x52, 0x11, 0x6e, 0x65, 0x77, 0x4a, 0x6f, 0x62, 0x41, 0x6c,
	0x6c, 0x6f, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x42, 0x13, 0x5a, 0x11, 0x55, 0x4e, 0x53,
	0x2f, 0x70, 0x62, 0x5f, 0x67, 0x65, 0x6e, 0x2f, 0x65, 0x76, 0x65, 0x6e, 0x74, 0x73, 0x62, 0x06,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_events_proto_rawDescOnce sync.Once
	file_events_proto_rawDescData = file_events_proto_rawDesc
)

func file_events_proto_rawDescGZIP() []byte {
	file_events_proto_rawDescOnce.Do(func() {
		file_events_proto_rawDescData = protoimpl.X.CompressGZIP(file_events_proto_rawDescData)
	})
	return file_events_proto_rawDescData
}

var file_events_proto_msgTypes = make([]protoimpl.MessageInfo, 5)
var file_events_proto_goTypes = []interface{}{
	(*RMRegisterResourceManagerEvent)(nil), // 0: pb_gen.RMRegisterResourceManagerEvent
	(*RMUpdateAllocationsEvent)(nil),       // 1: pb_gen.RMUpdateAllocationsEvent
	(*RMUpdateJobsEvent)(nil),              // 2: pb_gen.RMUpdateJobsEvent
	(*RMUpdateTimeEvent)(nil),              // 3: pb_gen.RMUpdateTimeEvent
	(*SSUpdateAllocationsEvent)(nil),       // 4: pb_gen.SSUpdateAllocationsEvent
	(*configs.RMConfiguration)(nil),        // 5: pb_gen.RMConfiguration
	(*objects.JobAllocation)(nil),          // 6: pb_gen.JobAllocation
	(*objects.Job)(nil),                    // 7: pb_gen.Job
}
var file_events_proto_depIdxs = []int32{
	5, // 0: pb_gen.RMRegisterResourceManagerEvent.configuration:type_name -> pb_gen.RMConfiguration
	6, // 1: pb_gen.RMUpdateAllocationsEvent.jobAllocations:type_name -> pb_gen.JobAllocation
	7, // 2: pb_gen.RMUpdateJobsEvent.newJobs:type_name -> pb_gen.Job
	6, // 3: pb_gen.SSUpdateAllocationsEvent.newJobAllocations:type_name -> pb_gen.JobAllocation
	4, // [4:4] is the sub-list for method output_type
	4, // [4:4] is the sub-list for method input_type
	4, // [4:4] is the sub-list for extension type_name
	4, // [4:4] is the sub-list for extension extendee
	0, // [0:4] is the sub-list for field type_name
}

func init() { file_events_proto_init() }
func file_events_proto_init() {
	if File_events_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_events_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*RMRegisterResourceManagerEvent); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_events_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*RMUpdateAllocationsEvent); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_events_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*RMUpdateJobsEvent); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_events_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*RMUpdateTimeEvent); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_events_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*SSUpdateAllocationsEvent); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_events_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   5,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_events_proto_goTypes,
		DependencyIndexes: file_events_proto_depIdxs,
		MessageInfos:      file_events_proto_msgTypes,
	}.Build()
	File_events_proto = out.File
	file_events_proto_rawDesc = nil
	file_events_proto_goTypes = nil
	file_events_proto_depIdxs = nil
}
