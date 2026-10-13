// Copyright 2026 Google LLC
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

package logtransform

import (
	"testing"

	policyv1alpha1 "github.com/GoogleCloudPlatform/opentelemetry-operations-collector/gen/go/policy/v1alpha1"
	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/pkg/googlepolicy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"google.golang.org/protobuf/proto"
)

func boolPtr(b bool) *bool       { return &b }
func strPtr(s string) *string    { return &s }

func TestValidation(t *testing.T) {
	_, err := NewPolicyFromProto(nil)
	require.ErrorIs(t, err, ErrNilProto)

	_, err = NewPolicyFromProto(&policyv1alpha1.LogTransformPolicy{})
	require.ErrorIs(t, err, ErrMissingID)

	_, err = NewPolicyFromProto(&policyv1alpha1.LogTransformPolicy{Id: "no-action"})
	require.ErrorIs(t, err, ErrMissingAction)

	_, err = NewPolicyFromProto(&policyv1alpha1.LogTransformPolicy{
		Id:     "missing-remove-target",
		Action: &policyv1alpha1.LogTransformPolicy_Remove{Remove: &policyv1alpha1.LogRemoveAction{}},
	})
	require.ErrorIs(t, err, ErrMissingTarget)

	_, err = NewPolicyFromProto(&policyv1alpha1.LogTransformPolicy{
		Id: "bad-redact-regex",
		Action: &policyv1alpha1.LogTransformPolicy_Redact{
			Redact: &policyv1alpha1.LogRedactAction{
				Target: &policyv1alpha1.LogFieldSelector{
					Target: &policyv1alpha1.LogFieldSelector_RecordField{
						RecordField: policyv1alpha1.LogRecordField_LOG_RECORD_FIELD_BODY,
					},
				},
				Regex: strPtr("[unclosed"),
			},
		},
	})
	require.Error(t, err)
}

func TestLogRemoveAction_BodyField(t *testing.T) {
	lr := plog.NewLogRecord()
	bodyMap := lr.Body().SetEmptyMap()
	evID := bodyMap.PutEmptyMap("event_id")
	evID.PutInt("id", 4624)
	bodyMap.PutStr("channel", "Security")
	bodyMap.PutStr("message", "Ein Konto wurde erfolgreich angemeldet.")
	ri := bodyMap.PutEmptyMap("rendering_info")
	ri.PutStr("culture", "de-DE")
	ri.PutStr("message", "Ein Konto wurde erfolgreich angemeldet.\n\nAntragsteller:\n\tSicherheits-ID:\t\tS-1-5-18")
	details := bodyMap.PutEmptyMap("details")
	details.PutStr("Anmeldetyp", "3")

	ctx := googlepolicy.LogContext{
		Record:   lr,
		Resource: pcommon.NewResource(),
		Scope:    pcommon.NewInstrumentationScope(),
	}

	// Non-matching policy (negative control): channel == "Application"
	nonMatchPol, err := NewPolicyFromProto(&policyv1alpha1.LogTransformPolicy{
		Id: "skip-non-matching",
		Matches: []*policyv1alpha1.LogMatcher{
			{
				Target: &policyv1alpha1.LogFieldSelector{
					Target: &policyv1alpha1.LogFieldSelector_BodyField{
						BodyField: &policyv1alpha1.AttributePath{Path: []string{"channel"}},
					},
				},
				Predicate: &policyv1alpha1.LogMatcher_Equals{
					Equals: &policyv1alpha1.Value{Value: &policyv1alpha1.Value_StringValue{StringValue: "Application"}},
				},
			},
		},
		Action: &policyv1alpha1.LogTransformPolicy_Remove{
			Remove: &policyv1alpha1.LogRemoveAction{
				Target: &policyv1alpha1.LogFieldSelector{
					Target: &policyv1alpha1.LogFieldSelector_BodyField{
						BodyField: &policyv1alpha1.AttributePath{Path: []string{"rendering_info"}},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	assert.False(t, nonMatchPol.TransformLog(ctx))
	_, exists := lr.Body().Map().Get("rendering_info")
	assert.True(t, exists, "rendering_info must remain when matcher does not match")

	// Matching policies (unconditional empty matches)
	for _, field := range []string{"rendering_info", "message", "details"} {
		pol, err := NewPolicyFromProto(&policyv1alpha1.LogTransformPolicy{
			Id: "remove-" + field,
			Action: &policyv1alpha1.LogTransformPolicy_Remove{
				Remove: &policyv1alpha1.LogRemoveAction{
					Target: &policyv1alpha1.LogFieldSelector{
						Target: &policyv1alpha1.LogFieldSelector_BodyField{
							BodyField: &policyv1alpha1.AttributePath{Path: []string{field}},
						},
					},
				},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, googlepolicy.StageRemove, pol.Stage())
		assert.True(t, pol.TransformLog(ctx), "expected %s to be removed", field)
		// Second removal is a no-op (fail-open)
		assert.False(t, pol.TransformLog(ctx), "expected second removal of %s to return false", field)
	}

	_, hasRI := lr.Body().Map().Get("rendering_info")
	_, hasMsg := lr.Body().Map().Get("message")
	_, hasDetails := lr.Body().Map().Get("details")
	assert.False(t, hasRI)
	assert.False(t, hasMsg)
	assert.False(t, hasDetails)

	// Verify event_id and channel are preserved
	idVal, ok := lr.Body().Map().Get("event_id")
	require.True(t, ok)
	idNum, ok := idVal.Map().Get("id")
	require.True(t, ok)
	assert.Equal(t, int64(4624), idNum.Int())
}

func TestLogRedactAction_RawXMLBody(t *testing.T) {
	rawXML := `<Event xmlns='http://schemas.microsoft.com/win/2004/08/events/event'><System><Provider Name='Microsoft-Windows-Security-Auditing'/><EventID>4624</EventID><Channel>Security</Channel></System><EventData><Data Name='TargetUserName'>s_admin</Data></EventData><RenderingInfo Culture='de-DE'><Message>Ein Konto wurde erfolgreich angemeldet.
Antragsteller:
	Sicherheits-ID:		S-1-5-18</Message><Level>Informationen</Level></RenderingInfo></Event>`

	lr := plog.NewLogRecord()
	lr.Body().SetStr(rawXML)

	ctx := googlepolicy.LogContext{
		Record:   lr,
		Resource: pcommon.NewResource(),
		Scope:    pcommon.NewInstrumentationScope(),
	}

	pol, err := NewPolicyFromProto(&policyv1alpha1.LogTransformPolicy{
		Id: "redact-wef-rendering-info",
		Action: &policyv1alpha1.LogTransformPolicy_Redact{
			Redact: &policyv1alpha1.LogRedactAction{
				Target: &policyv1alpha1.LogFieldSelector{
					Target: &policyv1alpha1.LogFieldSelector_RecordField{
						RecordField: policyv1alpha1.LogRecordField_LOG_RECORD_FIELD_BODY,
					},
				},
				Regex:       strPtr(`<RenderingInfo[^>]*>[\s\S]*?</RenderingInfo>`),
				Replacement: strPtr(""),
			},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, googlepolicy.StageRedact, pol.Stage())
	assert.True(t, pol.TransformLog(ctx))

	expected := `<Event xmlns='http://schemas.microsoft.com/win/2004/08/events/event'><System><Provider Name='Microsoft-Windows-Security-Auditing'/><EventID>4624</EventID><Channel>Security</Channel></System><EventData><Data Name='TargetUserName'>s_admin</Data></EventData></Event>`
	assert.Equal(t, expected, lr.Body().Str())

	// Second call when <RenderingInfo> is already stripped returns false
	assert.False(t, pol.TransformLog(ctx))
}

func TestLogAddAndRenameActions(t *testing.T) {
	lr := plog.NewLogRecord()
	lr.Attributes().PutStr("legacy_user", "alice")
	bodyMap := lr.Body().SetEmptyMap()
	bodyMap.PutInt("event_id", 4624)

	ctx := googlepolicy.LogContext{
		Record:   lr,
		Resource: pcommon.NewResource(),
		Scope:    pcommon.NewInstrumentationScope(),
	}

	// Rename legacy_user -> user.name
	renamePol, err := NewPolicyFromProto(&policyv1alpha1.LogTransformPolicy{
		Id: "rename-user",
		Action: &policyv1alpha1.LogTransformPolicy_Rename{
			Rename: &policyv1alpha1.LogRenameAction{
				From: &policyv1alpha1.LogFieldSelector{
					Target: &policyv1alpha1.LogFieldSelector_LogAttribute{
						LogAttribute: &policyv1alpha1.AttributePath{Path: []string{"legacy_user"}},
					},
				},
				To: &policyv1alpha1.LogFieldSelector{
					Target: &policyv1alpha1.LogFieldSelector_LogAttribute{
						LogAttribute: &policyv1alpha1.AttributePath{Path: []string{"user.name"}},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, googlepolicy.StageRename, renamePol.Stage())
	assert.True(t, renamePol.TransformLog(ctx))

	_, oldExists := lr.Attributes().Get("legacy_user")
	assert.False(t, oldExists)
	newVal, newExists := lr.Attributes().Get("user.name")
	require.True(t, newExists)
	assert.Equal(t, "alice", newVal.Str())

	// Add user.name with upsert: false should not overwrite "alice"
	addNoUpsert, err := NewPolicyFromProto(&policyv1alpha1.LogTransformPolicy{
		Id: "add-fallback-user",
		Action: &policyv1alpha1.LogTransformPolicy_Add{
			Add: &policyv1alpha1.LogAddAction{
				Target: &policyv1alpha1.LogFieldSelector{
					Target: &policyv1alpha1.LogFieldSelector_LogAttribute{
						LogAttribute: &policyv1alpha1.AttributePath{Path: []string{"user.name"}},
					},
				},
				Value:  &policyv1alpha1.Value{Value: &policyv1alpha1.Value_StringValue{StringValue: "unknown"}},
				Upsert: boolPtr(false),
			},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, googlepolicy.StageAdd, addNoUpsert.Stage())
	assert.False(t, addNoUpsert.TransformLog(ctx))
	newVal, _ = lr.Attributes().Get("user.name")
	assert.Equal(t, "alice", newVal.Str())

	// Fail-open rename when destination cannot be written (e.g. body_field on a string body)
	lrStr := plog.NewLogRecord()
	lrStr.Attributes().PutStr("keep_me", "important-value")
	lrStr.Body().SetStr("raw string body")
	ctxStr := googlepolicy.LogContext{
		Record:   lrStr,
		Resource: pcommon.NewResource(),
		Scope:    pcommon.NewInstrumentationScope(),
	}
	renameToBodyField, err := NewPolicyFromProto(&policyv1alpha1.LogTransformPolicy{
		Id: "rename-to-body-field-on-string-body",
		Action: &policyv1alpha1.LogTransformPolicy_Rename{
			Rename: &policyv1alpha1.LogRenameAction{
				From: &policyv1alpha1.LogFieldSelector{
					Target: &policyv1alpha1.LogFieldSelector_LogAttribute{
						LogAttribute: &policyv1alpha1.AttributePath{Path: []string{"keep_me"}},
					},
				},
				To: &policyv1alpha1.LogFieldSelector{
					Target: &policyv1alpha1.LogFieldSelector_BodyField{
						BodyField: &policyv1alpha1.AttributePath{Path: []string{"nested_target"}},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	assert.False(t, renameToBodyField.TransformLog(ctxStr))
	keptVal, stillThere := lrStr.Attributes().Get("keep_me")
	require.True(t, stillThere, "source attribute must not be lost when destination body_field fails open")
	assert.Equal(t, "important-value", keptVal.Str())

	// Self-rename is a no-op
	selfRename, err := NewPolicyFromProto(&policyv1alpha1.LogTransformPolicy{
		Id: "self-rename",
		Action: &policyv1alpha1.LogTransformPolicy_Rename{
			Rename: &policyv1alpha1.LogRenameAction{
				From: &policyv1alpha1.LogFieldSelector{
					Target: &policyv1alpha1.LogFieldSelector_LogAttribute{
						LogAttribute: &policyv1alpha1.AttributePath{Path: []string{"keep_me"}},
					},
				},
				To: &policyv1alpha1.LogFieldSelector{
					Target: &policyv1alpha1.LogFieldSelector_LogAttribute{
						LogAttribute: &policyv1alpha1.AttributePath{Path: []string{"keep_me"}},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	assert.False(t, selfRename.TransformLog(ctxStr))
}

func TestRegistryIntegration(t *testing.T) {
	// Load via MakePolicySetFromProtos
	pb := &policyv1alpha1.LogTransformPolicy{
		Id: "projects/dashpole-dev/locations/global/policySets/ps1/policies/wef-strip-ri",
		Action: &policyv1alpha1.LogTransformPolicy_Remove{
			Remove: &policyv1alpha1.LogRemoveAction{
				Target: &policyv1alpha1.LogFieldSelector{
					Target: &policyv1alpha1.LogFieldSelector_BodyField{
						BodyField: &policyv1alpha1.AttributePath{Path: []string{"rendering_info"}},
					},
				},
			},
		},
	}
	ps, err := googlepolicy.MakePolicySetFromProtos("rev-1", []proto.Message{pb})
	require.NoError(t, err)
	require.Len(t, ps.TransformationPolicies(), 1)

	// Load via MakePolicySet (authored map config)
	psMap, err := googlepolicy.MakePolicySet("rev-2", []map[string]any{
		{
			"id":   "wef-redact-ri",
			"type": "log_transform",
			"redact": map[string]any{
				"target": map[string]any{
					"record_field": "LOG_RECORD_FIELD_BODY",
				},
				"regex":       "<RenderingInfo[^>]*>[\\s\\S]*?</RenderingInfo>",
				"replacement": "",
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, psMap.TransformationPolicies(), 1)
}
