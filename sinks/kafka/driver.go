// Copyright 2015 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kafka

import (
	"net/url"
	"sync"
	"time"

	"k8s.io/klog"

	kafka_common "github.com/AliyunContainerService/kube-eventer/common/kafka"
	event_core "github.com/AliyunContainerService/kube-eventer/core"
	kube_api "k8s.io/api/core/v1"
)

type KafkaSinkPoint struct {
	EventKind            string
	EventMessage         string
	EventClusterName     string
	EventName            string
	EventNamespace       string
	EventReason          string
	EventSource          string
	EventType            string
	EventCount           int32
	EventCreateTimestamp time.Time
	EventFirstTimestamp  time.Time
	EventLastTimestamp   time.Time
	EventTimestamp       time.Time
}

type kafkaSink struct {
	kafka_common.KafkaClient
	sync.RWMutex
	clusterName string
}


func eventToPoint(event *kube_api.Event, clusterName string) (*KafkaSinkPoint, error) {

	if !event.EventTime {
		event.FirstTimestamp = event.EventTime
		event.LastTimestamp = event.EventTime
	}

	var sinkPoint = KafkaSinkPoint{
		EventKind:            event.InvolvedObject.Kind,
		EventMessage:         event.Message,
		EventName:            event.InvolvedObject.Name,
		EventNamespace:       event.InvolvedObject.Namespace,
		EventReason:          event.Reason,
		EventSource:          event.Source.Component,
		EventType:            event.Type,
		EventCount:           event.Count,
		EventCreateTimestamp: event.CreationTimestamp.Time.Local(),
		EventFirstTimestamp:  event.FirstTimestamp.Time.Local(),
		EventLastTimestamp:   event.LastTimestamp.Time.Local(),
		EventTimestamp:       event.LastTimestamp.Time.Local(),
		EventClusterName:     clusterName,
	}
	point := sinkPoint

	return &point, nil
}

func (sink *kafkaSink) ExportEvents(eventBatch *event_core.EventBatch) {
	sink.Lock()
	defer sink.Unlock()

	for _, event := range eventBatch.Events {
		point, err := eventToPoint(event, sink.clusterName)
		if err != nil {
			klog.Warningf("Failed to convert event to point: %v", err)
		}

		err = sink.ProduceKafkaMessage(*point)
		if err != nil {
			klog.Errorf("Failed to produce event message: %s", err)
		}
	}
}

func NewKafkaSink(uri *url.URL) (event_core.EventSink, error) {
	client, clusterName, err := kafka_common.NewKafkaClient(uri, kafka_common.EventsTopic)
	if err != nil {
		return nil, err
	}

	return &kafkaSink{
		KafkaClient: client,
		clusterName: clusterName,
	}, nil
}
