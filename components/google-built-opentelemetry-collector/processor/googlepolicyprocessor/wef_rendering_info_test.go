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

package googlepolicyprocessor

import (
	"context"
	"crypto/tls"
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"

	policyv1alpha1 "github.com/GoogleCloudPlatform/opentelemetry-operations-collector/gen/go/policy/v1alpha1"
	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/pkg/googlepolicy"
	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/pkg/googlepolicy/logtransform"
	ecpclient "github.com/googleapis/enterprise-certificate-proxy/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"

	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/processor/googlepolicyprocessor/internal/stanzawindows"
)

// Realistic pre-rendered German (Culture='de-DE') Windows Event Forwarding (WEF)
// XML payloads for Security Event 4624 (Logon) and Security Event 4688 (Process Creation)
// as stored in the ForwardedEvents channel on a Windows Event Collector (WEC) server
// when ContentFormat = RenderedText (Windows default).
const wefEvent4624GermanXML = `<Event xmlns='http://schemas.microsoft.com/win/2004/08/events/event'>
  <System>
    <Provider Name='Microsoft-Windows-Security-Auditing' Guid='{54849625-5478-4994-A5BA-3E3B0328C30D}'/>
    <EventID>4624</EventID>
    <Version>2</Version>
    <Level>0</Level>
    <Task>12544</Task>
    <Opcode>0</Opcode>
    <Keywords>0x8020000000000000</Keywords>
    <TimeCreated SystemTime='2026-04-06T14:22:10.4519821Z'/>
    <EventRecordID>9823411</EventRecordID>
    <Correlation ActivityID='{A1B2C3D4-E5F6-4789-9012-3456789ABCDE}'/>
    <Execution ProcessID='712' ThreadID='1940'/>
    <Channel>Security</Channel>
    <Computer>wks-muc-0412.corp.allianz.de</Computer>
    <Security/>
  </System>
  <EventData>
    <Data Name='SubjectUserSid'>S-1-5-18</Data>
    <Data Name='SubjectUserName'>WKS-MUC-0412$</Data>
    <Data Name='SubjectDomainName'>CORP</Data>
    <Data Name='SubjectLogonId'>0x3e7</Data>
    <Data Name='TargetUserSid'>S-1-5-21-3623811015-3361044348-30300820-4129</Data>
    <Data Name='TargetUserName'>lukas.mueller</Data>
    <Data Name='TargetDomainName'>CORP</Data>
    <Data Name='TargetLogonId'>0x18f4a2b</Data>
    <Data Name='LogonType'>3</Data>
    <Data Name='LogonProcessName'>Kerberos</Data>
    <Data Name='AuthenticationPackageName'>Kerberos</Data>
    <Data Name='WorkstationName'>WKS-MUC-0412</Data>
    <Data Name='LogonGuid'>{6B8E1C92-2F4A-4B9E-8C11-92837465ABCD}</Data>
    <Data Name='TransmittedServices'>-</Data>
    <Data Name='LmPackageName'>-</Data>
    <Data Name='KeyLength'>0</Data>
    <Data Name='ProcessId'>0x0</Data>
    <Data Name='ProcessName'>-</Data>
    <Data Name='IpAddress'>10.142.88.19</Data>
    <Data Name='IpPort'>54192</Data>
    <Data Name='ImpersonationLevel'>%%1833</Data>
    <Data Name='RestrictedAdminMode'>-</Data>
    <Data Name='TargetOutboundUserName'>-</Data>
    <Data Name='TargetOutboundDomainName'>-</Data>
    <Data Name='VirtualAccount'>%%1843</Data>
    <Data Name='TargetLinkedLogonId'>0x0</Data>
    <Data Name='ElevatedToken'>%%1842</Data>
  </EventData>
  <RenderingInfo Culture='de-DE'>
    <Message>Ein Konto wurde erfolgreich angemeldet.

Antragsteller:
	Sicherheits-ID:		S-1-5-18
	Kontoname:		WKS-MUC-0412$
	Kontodomäne:		CORP
	Anmelde-ID:		0x3E7

Anmeldeinformationen:
	Anmeldetyp:		3
	Eingeschränkter Administratormodus:	-
	Virtuelles Konto:		Nein
	Erhöhtes Token:		Ja

Identitätswechselebene:		Identitätswechsel

Neuer Anmeldevorgang:
	Sicherheits-ID:		S-1-5-21-3623811015-3361044348-30300820-4129
	Kontoname:		lukas.mueller
	Kontodomäne:		CORP
	Anmelde-ID:		0x18F4A2B
	Verknüpfte Anmelde-ID:		0x0
	Netzwerkkontoname:	-
	Netzwerkkontodomäne:	-
	Anmelde-GUID:		{6B8E1C92-2F4A-4B9E-8C11-92837465ABCD}

Prozessinformationen:
	Prozess-ID:		0x0
	Prozessname:		-

Netzwerkinformationen:
	Arbeitsstationsname:	WKS-MUC-0412
	Quellnetzwerkadresse:	10.142.88.19
	Quellport:		54192

Detaillierte Authentifizierungsinformationen:
	Anmeldeprozess:		Kerberos
	Authentifizierungspaket:	Kerberos
	Übertragene Dienste:	-
	Paketname (nur NTLM):	-
	Schlüssellänge:		0

Dieses Ereignis wird beim Erstellen einer Anmeldesitzung generiert. Es wird auf dem Computer generiert, auf den zugegriffen wurde.
Die Antragstellerfelder geben das Konto auf dem lokalen System an, von dem die Anmeldung angefordert wurde. Dies ist meistens ein Dienst wie der Serverdienst oder ein lokaler Prozess wie "Winlogon.exe" oder "Services.exe".
Das Feld "Anmeldetyp" gibt den Typ der erfolgten Anmeldung an. Die häufigsten Typen sind 2 (interaktiv) und 3 (Netzwerk).
Die Felder "Neuer Anmeldevorgang" geben das Konto an, für das die neue Anmeldung erstellt wurde, d. h. das Konto, das angemeldet wurde.
Die Netzwerkfelder geben an, woher eine Remoteanmeldeanforderung stammt. "Arbeitsstationsname" ist nicht immer verfügbar und kann in einigen Fällen leer bleiben.
Die Felder für die Identitätswechselebene geben an, in welchem Umfang ein Prozess in der Anmeldesitzung die Identität eines Clients annehmen kann.
Die Felder für Authentifizierungsinformationen enthalten detaillierte Informationen zu dieser speziellen Anmeldeanforderung.
	- "Anmelde-GUID" ist eine eindeutige Kennung, die verwendet werden kann, um dieses Ereignis mit einem KDC-Ereignis zu korrelieren.
	- "Übertragene Dienste" gibt an, welche Zwischendienste an dieser Anmeldeanforderung beteiligt waren.
	- "Paketname" gibt an, welches Unterprotokoll unter den NTLM-Protokollen verwendet wurde.
	- "Schlüssellänge" gibt die Länge des generierten Sitzungsschlüssels an. Dieser Wert ist 0, wenn kein Sitzungsschlüssel angefordert wurde.</Message>
    <Level>Informationen</Level>
    <Task>Anmeldung</Task>
    <Opcode>Info</Opcode>
    <Channel>Sicherheit</Channel>
    <Provider>Microsoft Windows security auditing.</Provider>
    <Keywords>
      <Keyword>Überwachung erfolgreich</Keyword>
    </Keywords>
  </RenderingInfo>
</Event>`

const wefEvent4688GermanXML = `<Event xmlns='http://schemas.microsoft.com/win/2004/08/events/event'>
  <System>
    <Provider Name='Microsoft-Windows-Security-Auditing' Guid='{54849625-5478-4994-A5BA-3E3B0328C30D}'/>
    <EventID>4688</EventID>
    <Version>2</Version>
    <Level>0</Level>
    <Task>13312</Task>
    <Opcode>0</Opcode>
    <Keywords>0x8020000000000000</Keywords>
    <TimeCreated SystemTime='2026-04-06T14:22:15.8124903Z'/>
    <EventRecordID>9823499</EventRecordID>
    <Correlation/>
    <Execution ProcessID='4' ThreadID='812'/>
    <Channel>Security</Channel>
    <Computer>wks-muc-0412.corp.allianz.de</Computer>
    <Security/>
  </System>
  <EventData>
    <Data Name='SubjectUserSid'>S-1-5-21-3623811015-3361044348-30300820-4129</Data>
    <Data Name='SubjectUserName'>lukas.mueller</Data>
    <Data Name='SubjectDomainName'>CORP</Data>
    <Data Name='SubjectLogonId'>0x18f4a2b</Data>
    <Data Name='NewProcessId'>0x24b8</Data>
    <Data Name='NewProcessName'>C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe</Data>
    <Data Name='TokenElevationType'>%%1937</Data>
    <Data Name='ProcessId'>0x11c4</Data>
    <Data Name='CommandLine'>powershell.exe -NoProfile -ExecutionPolicy Bypass -File C:\Admin\Scripts\Collect-Diagnostics.ps1</Data>
    <Data Name='TargetUserSid'>S-1-0-0</Data>
    <Data Name='TargetUserName'>-</Data>
    <Data Name='TargetDomainName'>-</Data>
    <Data Name='TargetLogonId'>0x0</Data>
    <Data Name='ParentProcessName'>C:\Windows\explorer.exe</Data>
    <Data Name='MandatoryLabel'>S-1-16-12288</Data>
  </EventData>
  <RenderingInfo Culture='de-DE'>
    <Message>Ein neuer Prozess wurde erstellt.

Antragsteller:
	Sicherheits-ID:		S-1-5-21-3623811015-3361044348-30300820-4129
	Kontoname:		lukas.mueller
	Kontodomäne:		CORP
	Anmelde-ID:		0x18F4A2B

Zielkonto:
	Sicherheits-ID:		S-1-0-0
	Kontoname:		-
	Kontodomäne:		-
	Anmelde-ID:		0x0

Prozessinformationen:
	Neue Prozess-ID:		0x24B8
	Neuer Prozessname:	C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe
	Art der Tokenerhöhung:	TokenElevationTypeFull (2)
	Verbindliche Beschriftung:		S-1-16-12288
	Erstellerprozess-ID:	0x11C4
	Erstellerprozessname:	C:\Windows\explorer.exe
	Prozessbefehlszeile:	powershell.exe -NoProfile -ExecutionPolicy Bypass -File C:\Admin\Scripts\Collect-Diagnostics.ps1

Die Art der Tokenerhöhung gibt die Art des Tokens an, das dem neuen Prozess gemäß der Richtlinie für die Benutzerkontensteuerung zugewiesen wurde.

Typ 1 ist ein vollständiges Token ohne entfernte Berechtigungen oder deaktivierte Gruppen. Ein vollständiges Token wird nur verwendet, wenn die Benutzerkontensteuerung deaktiviert ist oder wenn der Benutzer das integrierte Administratorkonto oder ein Dienstkonto ist.

Typ 2 ist ein erhöhtes Token ohne entfernte Berechtigungen oder deaktivierte Gruppen. Ein erhöhtes Token wird verwendet, wenn die Benutzerkontensteuerung aktiviert ist und der Benutzer wählt, das Programm mit "Als Administrator ausführen" zu starten. Es wird auch verwendet, wenn eine Anwendung so konfiguriert ist, dass sie immer Administratorrechte erfordert, oder eine Anwendung immer maximale Berechtigungen erfordert und der Benutzer Mitglied der Gruppe "Administratoren" ist.

Typ 3 ist ein eingeschränktes Token, bei dem Administratorberechtigungen entfernt und Administratorgruppen deaktiviert wurden. Das eingeschränkte Token wird verwendet, wenn die Benutzerkontensteuerung aktiviert ist, die Anwendung keine Administratorrechte erfordert und der Benutzer nicht "Als Administrator ausführen" auswählt.</Message>
    <Level>Informationen</Level>
    <Task>Prozesserstellung</Task>
    <Opcode>Info</Opcode>
    <Channel>Sicherheit</Channel>
    <Provider>Microsoft Windows security auditing.</Provider>
    <Keywords>
      <Keyword>Überwachung erfolgreich</Keyword>
    </Keywords>
  </RenderingInfo>
</Event>`

func newWEFEvaluator(t testing.TB, protos ...*policyv1alpha1.LogTransformPolicy) *Evaluator {
	t.Helper()
	policies := make([]googlepolicy.TransformationPolicy, 0, len(protos))
	for _, p := range protos {
		pol, err := logtransform.NewPolicyFromProto(p)
		require.NoError(t, err)
		require.NoError(t, pol.Validate())
		policies = append(policies, pol)
	}
	ev, err := NewEvaluator(policies)
	require.NoError(t, err)
	return ev
}

// buildConfig1BPolicies returns the 2-action LogRemoveAction rules currently shown in
// the Companion Design Doc's ASCII diagram (removing body_field(["rendering_info"]) and
// body_field(["message"]), omitting body_field(["details"])).
func buildConfig1BPolicies() []*policyv1alpha1.LogTransformPolicy {
	return []*policyv1alpha1.LogTransformPolicy{
		{
			Id: "wef-remove-rendering-info",
			Action: &policyv1alpha1.LogTransformPolicy_Remove{
				Remove: &policyv1alpha1.LogRemoveAction{
					Target: &policyv1alpha1.LogFieldSelector{
						Target: &policyv1alpha1.LogFieldSelector_BodyField{
							BodyField: &policyv1alpha1.AttributePath{Path: []string{"rendering_info"}},
						},
					},
				},
			},
		},
		{
			Id: "wef-remove-message",
			Action: &policyv1alpha1.LogTransformPolicy_Remove{
				Remove: &policyv1alpha1.LogRemoveAction{
					Target: &policyv1alpha1.LogFieldSelector{
						Target: &policyv1alpha1.LogFieldSelector_BodyField{
							BodyField: &policyv1alpha1.AttributePath{Path: []string{"message"}},
						},
					},
				},
			},
		},
	}
}

// buildConfig1CPolicies returns the complete 3-action LogRemoveAction rules required
// for structured mode (raw: false): removing body_field(["rendering_info"]),
// body_field(["message"]), and body_field(["details"]).
func buildConfig1CPolicies() []*policyv1alpha1.LogTransformPolicy {
	return append(buildConfig1BPolicies(), &policyv1alpha1.LogTransformPolicy{
		Id: "wef-remove-details",
		Action: &policyv1alpha1.LogTransformPolicy_Remove{
			Remove: &policyv1alpha1.LogRemoveAction{
				Target: &policyv1alpha1.LogFieldSelector{
					Target: &policyv1alpha1.LogFieldSelector_BodyField{
						BodyField: &policyv1alpha1.AttributePath{Path: []string{"details"}},
					},
				},
			},
		},
	})
}

// buildConfig2BPolicies returns the LogRedactAction rule for raw XML mode (raw: true).
func buildConfig2BPolicies() []*policyv1alpha1.LogTransformPolicy {
	regex := `<RenderingInfo[^>]*>[\s\S]*?</RenderingInfo>`
	replacement := ""
	return []*policyv1alpha1.LogTransformPolicy{
		{
			Id: "wef-redact-raw-rendering-info",
			Action: &policyv1alpha1.LogTransformPolicy_Redact{
				Redact: &policyv1alpha1.LogRedactAction{
					Target: &policyv1alpha1.LogFieldSelector{
						Target: &policyv1alpha1.LogFieldSelector_RecordField{
							RecordField: policyv1alpha1.LogRecordField_LOG_RECORD_FIELD_BODY,
						},
					},
					Regex:       &regex,
					Replacement: &replacement,
				},
			},
		},
	}
}

// buildEventIDEnrichmentPolicies returns optional LogAddAction enrichment policies
// mapping EventID 4624 -> event.action="user-login" and 4688 -> event.action="process-created".
func buildEventIDEnrichmentPolicies() []*policyv1alpha1.LogTransformPolicy {
	upsertTrue := true
	return []*policyv1alpha1.LogTransformPolicy{
		{
			Id: "enrich-event-4624",
			Matches: []*policyv1alpha1.LogMatcher{
				{
					Target: &policyv1alpha1.LogFieldSelector{
						Target: &policyv1alpha1.LogFieldSelector_BodyField{
							BodyField: &policyv1alpha1.AttributePath{Path: []string{"event_id", "id"}},
						},
					},
					Predicate: &policyv1alpha1.LogMatcher_Equals{
						Equals: &policyv1alpha1.Value{Value: &policyv1alpha1.Value_IntValue{IntValue: 4624}},
					},
				},
			},
			Action: &policyv1alpha1.LogTransformPolicy_Add{
				Add: &policyv1alpha1.LogAddAction{
					Target: &policyv1alpha1.LogFieldSelector{
						Target: &policyv1alpha1.LogFieldSelector_LogAttribute{
							LogAttribute: &policyv1alpha1.AttributePath{Path: []string{"event.action"}},
						},
					},
					Value:  &policyv1alpha1.Value{Value: &policyv1alpha1.Value_StringValue{StringValue: "user-login"}},
					Upsert: &upsertTrue,
				},
			},
		},
		{
			Id: "enrich-event-4688",
			Matches: []*policyv1alpha1.LogMatcher{
				{
					Target: &policyv1alpha1.LogFieldSelector{
						Target: &policyv1alpha1.LogFieldSelector_BodyField{
							BodyField: &policyv1alpha1.AttributePath{Path: []string{"event_id", "id"}},
						},
					},
					Predicate: &policyv1alpha1.LogMatcher_Equals{
						Equals: &policyv1alpha1.Value{Value: &policyv1alpha1.Value_IntValue{IntValue: 4688}},
					},
				},
			},
			Action: &policyv1alpha1.LogTransformPolicy_Add{
				Add: &policyv1alpha1.LogAddAction{
					Target: &policyv1alpha1.LogFieldSelector{
						Target: &policyv1alpha1.LogFieldSelector_LogAttribute{
							LogAttribute: &policyv1alpha1.AttributePath{Path: []string{"event.action"}},
						},
					},
					Value:  &policyv1alpha1.Value{Value: &policyv1alpha1.Value_StringValue{StringValue: "process-created"}},
					Upsert: &upsertTrue,
				},
			},
		},
	}
}

func parseWEFToLogs(t testing.TB, xmlStr string, raw bool) plog.Logs {
	t.Helper()
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	sl := rl.ScopeLogs().AppendEmpty()
	lr := sl.LogRecords().AppendEmpty()
	require.NoError(t, stanzawindows.PopulateLogRecord(lr, []byte(xmlStr), raw, stanzawindows.EventDataFormatMap))
	return ld
}

func marshaledSize(t testing.TB, ld plog.Logs) int {
	t.Helper()
	var m plog.ProtoMarshaler
	buf, err := m.MarshalLogs(ld)
	require.NoError(t, err)
	return len(buf)
}

func TestWEFRenderingInfo_EmpiricalComparison(t *testing.T) {
	eval1A := newWEFEvaluator(t)
	eval1B := newWEFEvaluator(t, buildConfig1BPolicies()...)
	eval1C := newWEFEvaluator(t, buildConfig1CPolicies()...)
	eval1CEnriched := newWEFEvaluator(t, append(buildConfig1CPolicies(), buildEventIDEnrichmentPolicies()...)...)
	eval2A := newWEFEvaluator(t)
	eval2B := newWEFEvaluator(t, buildConfig2BPolicies()...)

	type eventCase struct {
		name               string
		eventID            int64
		xmlPayload         string
		expectedAction     string
		expectedDataCount  int
		expectedDataSample map[string]string
	}

	cases := []eventCase{
		{
			name:              "Security_Event_4624_Logon_deDE",
			eventID:           4624,
			xmlPayload:        wefEvent4624GermanXML,
			expectedAction:    "user-login",
			expectedDataCount: 27,
			expectedDataSample: map[string]string{
				"TargetUserName": "lukas.mueller",
				"LogonType":      "3",
				"IpAddress":      "10.142.88.19",
			},
		},
		{
			name:              "Security_Event_4688_ProcessCreate_deDE",
			eventID:           4688,
			xmlPayload:        wefEvent4688GermanXML,
			expectedAction:    "process-created",
			expectedDataCount: 15,
			expectedDataSample: map[string]string{
				"SubjectUserName": "lukas.mueller",
				"NewProcessName":  `C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe`,
				"CommandLine":     `powershell.exe -NoProfile -ExecutionPolicy Bypass -File C:\Admin\Scripts\Collect-Diagnostics.ps1`,
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rawXMLBytes := len(tc.xmlPayload)

			// 1A: Structured Baseline (raw: false, no LogTransformPolicy)
			ld1A := parseWEFToLogs(t, tc.xmlPayload, false)
			eval1A.TransformLogs(ld1A)
			size1A := marshaledSize(t, ld1A)
			body1A := ld1A.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Body().Map()
			_, hasRI1A := body1A.Get("rendering_info")
			_, hasMsg1A := body1A.Get("message")
			details1A, hasDetails1A := body1A.Get("details")
			assert.True(t, hasRI1A, "1A must retain rendering_info")
			assert.True(t, hasMsg1A, "1A must retain message")
			assert.True(t, hasDetails1A, "1A must retain details created by parseSecurity")
			assert.Equal(t, pcommon.ValueTypeMap, details1A.Type())

			// 1B: Structured Partial Strip (2-action: rendering_info + message only)
			ld1B := parseWEFToLogs(t, tc.xmlPayload, false)
			eval1B.TransformLogs(ld1B)
			size1B := marshaledSize(t, ld1B)
			body1B := ld1B.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Body().Map()
			_, hasRI1B := body1B.Get("rendering_info")
			_, hasMsg1B := body1B.Get("message")
			details1B, hasDetails1B := body1B.Get("details")
			assert.False(t, hasRI1B, "1B removes rendering_info")
			assert.False(t, hasMsg1B, "1B removes message")
			assert.True(t, hasDetails1B, "1B still retains body.details (~1.5-2.0 KB of German prose from parseSecurity)")
			assert.Greater(t, details1B.Map().Len(), 3)

			// 1C: Structured Full Strip (3-action: rendering_info + message + details)
			ld1C := parseWEFToLogs(t, tc.xmlPayload, false)
			eval1C.TransformLogs(ld1C)
			size1C := marshaledSize(t, ld1C)
			body1C := ld1C.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Body().Map()
			_, hasRI1C := body1C.Get("rendering_info")
			_, hasMsg1C := body1C.Get("message")
			_, hasDetails1C := body1C.Get("details")
			assert.False(t, hasRI1C, "1C removes rendering_info")
			assert.False(t, hasMsg1C, "1C removes message")
			assert.False(t, hasDetails1C, "1C removes details")

			// Verify 100% preservation of <System> and <EventData> in 1C
			eventIDMap, ok := body1C.Get("event_id")
			require.True(t, ok)
			idVal, ok := eventIDMap.Map().Get("id")
			require.True(t, ok)
			assert.Equal(t, tc.eventID, idVal.Int())

			provMap, ok := body1C.Get("provider")
			require.True(t, ok)
			provName, ok := provMap.Map().Get("name")
			require.True(t, ok)
			assert.Equal(t, "Microsoft-Windows-Security-Auditing", provName.Str())

			compVal, ok := body1C.Get("computer")
			require.True(t, ok)
			assert.Equal(t, "wks-muc-0412.corp.allianz.de", compVal.Str())

			chanVal, ok := body1C.Get("channel")
			require.True(t, ok)
			assert.Equal(t, "Security", chanVal.Str())

			eventData1A, ok := body1A.Get("event_data")
			require.True(t, ok)
			eventDataVal, ok := body1C.Get("event_data")
			require.True(t, ok)
			assert.Equal(t, tc.expectedDataCount, eventDataVal.Map().Len(), "1C must preserve all %d EventData keys", tc.expectedDataCount)
			assert.Equal(t, eventData1A.AsRaw(), eventDataVal.AsRaw(), "1C event_data map must be 100%% identical to 1A baseline")
			for k, expectedV := range tc.expectedDataSample {
				gotV, exists := eventDataVal.Map().Get(k)
				require.True(t, exists, "missing EventData key %s", k)
				assert.Equal(t, expectedV, gotV.Str())
			}

			// Verify 1C + Event ID Enrichment (LogAddAction + LogRemoveAction)
			ld1CEnriched := parseWEFToLogs(t, tc.xmlPayload, false)
			eval1CEnriched.TransformLogs(ld1CEnriched)
			lr1CEnriched := ld1CEnriched.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
			actionAttr, hasAction := lr1CEnriched.Attributes().Get("event.action")
			require.True(t, hasAction)
			assert.Equal(t, tc.expectedAction, actionAttr.Str())

			// 2A: Raw XML Baseline (raw: true, no LogTransformPolicy)
			ld2A := parseWEFToLogs(t, tc.xmlPayload, true)
			eval2A.TransformLogs(ld2A)
			size2A := marshaledSize(t, ld2A)
			rawBody2A := ld2A.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Body().Str()
			assert.Contains(t, rawBody2A, `<RenderingInfo Culture='de-DE'>`)

			// 2B: Raw XML Redact (raw: true, LogRedactAction stripping <RenderingInfo>...</RenderingInfo>)
			ld2B := parseWEFToLogs(t, tc.xmlPayload, true)
			eval2B.TransformLogs(ld2B)
			size2B := marshaledSize(t, ld2B)
			rawBody2B := ld2B.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Body().Str()
			assert.NotContains(t, rawBody2B, "<RenderingInfo")
			assert.NotContains(t, rawBody2B, "</RenderingInfo>")

			// Verify 2B produces valid XML with <System> and <EventData> 100% intact
			origParsed, err := stanzawindows.UnmarshalEventXML([]byte(tc.xmlPayload))
			require.NoError(t, err)
			origXML, ok := origParsed.(*stanzawindows.EventXML)
			require.True(t, ok)

			reparsed, err := stanzawindows.UnmarshalEventXML([]byte(rawBody2B))
			require.NoError(t, err, "redacted XML in 2B must remain valid EventXML")
			evXML, ok := reparsed.(*stanzawindows.EventXML)
			require.True(t, ok)
			assert.Nil(t, evXML.RenderingInfo)
			assert.Equal(t, uint32(tc.eventID), evXML.EventID.ID)
			assert.Equal(t, "Microsoft-Windows-Security-Auditing", evXML.Provider.Name)
			assert.Equal(t, "wks-muc-0412.corp.allianz.de", evXML.Computer)
			assert.Len(t, evXML.EventData.Data, tc.expectedDataCount, "2B must preserve all %d EventData elements", tc.expectedDataCount)
			assert.Equal(t, origXML.EventData, evXML.EventData, "2B EventData must be 100%% identical to original XML")

			reduction1B := 100.0 * float64(size1A-size1B) / float64(size1A)
			reduction1C := 100.0 * float64(size1A-size1C) / float64(size1A)
			reduction2B := 100.0 * float64(size2A-size2B) / float64(size2A)
			residualDetailsBytes := size1B - size1C

			t.Logf("[%s] Raw XML input=%d B | 1A(Structured Baseline)=%d B | 1B(2-Action Partial)=%d B (-%.1f%%, residual details=%d B) | 1C(3-Action Full)=%d B (-%.1f%%) | 2A(Raw Baseline)=%d B | 2B(Raw Redact)=%d B (-%.1f%%)",
				tc.name, rawXMLBytes, size1A, size1B, reduction1B, residualDetailsBytes, size1C, reduction1C, size2A, size2B, reduction2B)

			// Falsifiable exit gate assertions:
			// - In structured mode (1C vs 1A), stripping rendering_info + message + details removes both
			//   duplicated copies of the German message, reducing serialized OTLP LogRecord size by >= 75%.
			// - In raw XML mode (2B vs 2A), stripping 100% of <RenderingInfo> removes >= 55% of total event bytes
			//   (100% of the ~2.3-2.8 KB RenderingInfo block, leaving only the ~1.6-2.0 KB <System>+<EventData> XML).
			assert.GreaterOrEqual(t, reduction1C, 75.0, "1C must reduce serialized OTLP LogRecord size by >= 75%%")
			assert.GreaterOrEqual(t, reduction2B, 55.0, "2B must reduce serialized OTLP LogRecord size by >= 55%% (100%% of <RenderingInfo>)")
			assert.GreaterOrEqual(t, residualDetailsBytes, 1400, "1B leaves >= 1.4 KB of German details prose when body_field([\"details\"]) is omitted")
		})
	}
}

func TestWEFRenderingInfo_LiveCloudLoggingExport(t *testing.T) {
	tokenBytes, err := exec.Command("gcloud", "auth", "print-access-token").Output()
	require.NoError(t, err, "failed to obtain access token via gcloud auth print-access-token")
	token := strings.TrimSpace(string(tokenBytes))
	require.NotEmpty(t, token)

	const projectID = "dashpole-dev"
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// Build a batch containing both transformed structured WEF events (1C + Event ID enrichment)
	// and transformed raw XML WEF events (2B).
	evalStructured := newWEFEvaluator(t, append(buildConfig1CPolicies(), buildEventIDEnrichmentPolicies()...)...)
	evalRaw := newWEFEvaluator(t, buildConfig2BPolicies()...)

	ldStructured := plog.NewLogs()
	rlS := ldStructured.ResourceLogs().AppendEmpty()
	rlS.Resource().Attributes().PutStr("gcp.project_id", projectID)
	rlS.Resource().Attributes().PutStr("service.name", "tango-tab5-wef-prototype")
	slS := rlS.ScopeLogs().AppendEmpty()
	slS.Scope().SetName("windowseventlogreceiver/wef")

	now := time.Now().UTC()
	for i, xmlStr := range []string{wefEvent4624GermanXML, wefEvent4688GermanXML} {
		lr := slS.LogRecords().AppendEmpty()
		require.NoError(t, stanzawindows.PopulateLogRecord(lr, []byte(xmlStr), false, stanzawindows.EventDataFormatMap))
		// Use current timestamp so Google Cloud Logging accepts the entry within its retention window.
		lr.SetTimestamp(pcommon.NewTimestampFromTime(now.Add(time.Duration(i) * time.Millisecond)))
		lr.SetObservedTimestamp(pcommon.NewTimestampFromTime(now.Add(time.Duration(i) * time.Millisecond)))
		lr.Attributes().PutStr("wef.mode", "1C_structured_full_strip")
	}
	evalStructured.TransformLogs(ldStructured)

	ldRaw := plog.NewLogs()
	rlR := ldRaw.ResourceLogs().AppendEmpty()
	rlR.Resource().Attributes().PutStr("gcp.project_id", projectID)
	rlR.Resource().Attributes().PutStr("service.name", "tango-tab5-wef-prototype")
	slR := rlR.ScopeLogs().AppendEmpty()
	slR.Scope().SetName("windowseventlogreceiver/wef-raw")
	for i, xmlStr := range []string{wefEvent4624GermanXML, wefEvent4688GermanXML} {
		lr := slR.LogRecords().AppendEmpty()
		require.NoError(t, stanzawindows.PopulateLogRecord(lr, []byte(xmlStr), true, stanzawindows.EventDataFormatMap))
		lr.SetTimestamp(pcommon.NewTimestampFromTime(now.Add(time.Duration(10+i) * time.Millisecond)))
		lr.SetObservedTimestamp(pcommon.NewTimestampFromTime(now.Add(time.Duration(10+i) * time.Millisecond)))
		lr.Attributes().PutStr("wef.mode", "2B_raw_xml_redact")
	}
	evalRaw.TransformLogs(ldRaw)

	// Merge into a single ExportLogsServiceRequest batch for Google Cloud Telemetry API.
	exportBatch := plog.NewLogs()
	ldStructured.ResourceLogs().MoveAndAppendTo(exportBatch.ResourceLogs())
	ldRaw.ResourceLogs().MoveAndAppendTo(exportBatch.ResourceLogs())

	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}
	endpoint := "telemetry.googleapis.com:443"
	if ecpKey, ecpErr := ecpclient.Cred(""); ecpErr == nil {
		defer ecpKey.Close()
		tlsCfg.Certificates = []tls.Certificate{{
			Certificate: ecpKey.CertificateChain(),
			PrivateKey:  ecpKey,
		}}
		endpoint = "telemetry.mtls.googleapis.com:443"
	}

	conn, err := grpc.NewClient(
		endpoint,
		grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)),
	)
	require.NoError(t, err)
	defer conn.Close()

	client := plogotlp.NewGRPCClient(conn)
	outCtx := metadata.NewOutgoingContext(ctx, metadata.Pairs(
		"authorization", fmt.Sprintf("Bearer %s", token),
		"x-goog-user-project", projectID,
	))

	req := plogotlp.NewExportRequestFromLogs(exportBatch)
	resp, err := client.Export(outCtx, req)
	require.NoError(t, err, "live OTLP gRPC export to %s in %s failed", endpoint, projectID)
	assert.Equal(t, int64(0), resp.PartialSuccess().RejectedLogRecords(),
		"%s rejected log records: %s", endpoint, resp.PartialSuccess().ErrorMessage())
	t.Logf("Successfully exported 4 transformed WEF log records (2 structured 1C + 2 raw XML 2B) to %s in project %s (rejected_log_records=%d, error_message=%q)",
		endpoint, projectID, resp.PartialSuccess().RejectedLogRecords(), resp.PartialSuccess().ErrorMessage())
}

func BenchmarkWEFRenderingInfo(b *testing.B) {
	eval1A := newWEFEvaluator(b)
	eval1B := newWEFEvaluator(b, buildConfig1BPolicies()...)
	eval1C := newWEFEvaluator(b, buildConfig1CPolicies()...)
	eval2A := newWEFEvaluator(b)
	eval2B := newWEFEvaluator(b, buildConfig2BPolicies()...)

	baseStructured := parseWEFToLogs(b, wefEvent4624GermanXML, false)
	baseRaw := parseWEFToLogs(b, wefEvent4624GermanXML, true)

	b.Run("1A_StructuredBaseline_NoOp", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			ld := plog.NewLogs()
			baseStructured.CopyTo(ld)
			eval1A.TransformLogs(ld)
		}
	})

	b.Run("1B_StructuredPartialStrip_2Action", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			ld := plog.NewLogs()
			baseStructured.CopyTo(ld)
			eval1B.TransformLogs(ld)
		}
	})

	b.Run("1C_StructuredFullStrip_3Action", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			ld := plog.NewLogs()
			baseStructured.CopyTo(ld)
			eval1C.TransformLogs(ld)
		}
	})

	b.Run("2A_RawXMLBaseline_NoOp", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			ld := plog.NewLogs()
			baseRaw.CopyTo(ld)
			eval2A.TransformLogs(ld)
		}
	})

	b.Run("2B_RawXMLRedact_Regex", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			ld := plog.NewLogs()
			baseRaw.CopyTo(ld)
			eval2B.TransformLogs(ld)
		}
	})
}
