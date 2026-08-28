// SPDX-License-Identifier: Apache-2.0

package telemetry

import (
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/protobuf/proto"
)

func parseGoogleRetryInfo(body []byte) time.Duration {
	var status statuspb.Status
	if err := proto.Unmarshal(body, &status); err != nil {
		return -1
	}
	for _, detail := range status.Details {
		var retry errdetails.RetryInfo
		if err := detail.UnmarshalTo(&retry); err != nil || retry.RetryDelay == nil {
			continue
		}
		if err := retry.RetryDelay.CheckValid(); err != nil {
			continue
		}
		delay := retry.RetryDelay.AsDuration()
		if delay >= 0 {
			return delay
		}
	}
	return -1
}
