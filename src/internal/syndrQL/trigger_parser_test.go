package syndrQL

import (
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/pkg/constants"
	"testing"
)

func TestParseCreateTrigger_SingleEvent(t *testing.T) {
	input := `CREATE TRIGGER "audit_insert" ON BUNDLE "Orders" BEFORE INSERT FOR EACH DOCUMENT EXECUTE (ADD DOCUMENT TO BUNDLE "AuditLog" WITH ({"action" = "insert"}))`
	parser, err := NewTriggerParser(input)
	if err != nil {
		t.Fatalf("NewTriggerParser failed: %v", err)
	}

	cmd, err := parser.ParseCreateTrigger()
	if err != nil {
		t.Fatalf("ParseCreateTrigger failed: %v", err)
	}

	if cmd.TriggerName != "audit_insert" {
		t.Errorf("expected trigger name 'audit_insert', got %q", cmd.TriggerName)
	}
	if cmd.BundleName != "Orders" {
		t.Errorf("expected bundle name 'Orders', got %q", cmd.BundleName)
	}
	if cmd.Timing != models.TRIGGER_TIMING_BEFORE {
		t.Errorf("expected BEFORE timing, got %d", cmd.Timing)
	}
	if cmd.Events != models.TRIGGER_EVENT_INSERT {
		t.Errorf("expected INSERT event, got %d", cmd.Events)
	}
	if cmd.Priority != constants.DefaultTriggerPriority {
		t.Errorf("expected default priority %d, got %d", constants.DefaultTriggerPriority, cmd.Priority)
	}
	if cmd.Body == "" {
		t.Error("expected non-empty trigger body")
	}
}

func TestParseCreateTrigger_MultiEvent(t *testing.T) {
	input := `CREATE TRIGGER "track_changes" ON BUNDLE "Products" AFTER INSERT, UPDATE, DELETE FOR EACH DOCUMENT EXECUTE (ADD DOCUMENT TO BUNDLE "ChangeLog" WITH ({"event" = "change"}))`
	parser, err := NewTriggerParser(input)
	if err != nil {
		t.Fatalf("NewTriggerParser failed: %v", err)
	}

	cmd, err := parser.ParseCreateTrigger()
	if err != nil {
		t.Fatalf("ParseCreateTrigger failed: %v", err)
	}

	expectedEvents := models.TRIGGER_EVENT_INSERT | models.TRIGGER_EVENT_UPDATE | models.TRIGGER_EVENT_DELETE
	if cmd.Events != expectedEvents {
		t.Errorf("expected all three events (bitmask %d), got %d", expectedEvents, cmd.Events)
	}
}

func TestParseCreateTrigger_WithWhenClause(t *testing.T) {
	input := `CREATE TRIGGER "check_status" ON BUNDLE "Orders" BEFORE UPDATE FOR EACH DOCUMENT WHEN (NEW."status" != OLD."status") EXECUTE (ADD DOCUMENT TO BUNDLE "StatusLog" WITH ({"change" = "true"}))`
	parser, err := NewTriggerParser(input)
	if err != nil {
		t.Fatalf("NewTriggerParser failed: %v", err)
	}

	cmd, err := parser.ParseCreateTrigger()
	if err != nil {
		t.Fatalf("ParseCreateTrigger failed: %v", err)
	}

	if cmd.WhenClause == "" {
		t.Error("expected non-empty WHEN clause")
	}
}

func TestParseCreateTrigger_WithPriority(t *testing.T) {
	input := `CREATE TRIGGER "high_priority" ON BUNDLE "Orders" BEFORE INSERT FOR EACH DOCUMENT PRIORITY 100 EXECUTE (ADD DOCUMENT TO BUNDLE "Log" WITH ({"p" = "100"}))`
	parser, err := NewTriggerParser(input)
	if err != nil {
		t.Fatalf("NewTriggerParser failed: %v", err)
	}

	cmd, err := parser.ParseCreateTrigger()
	if err != nil {
		t.Fatalf("ParseCreateTrigger failed: %v", err)
	}

	if cmd.Priority != 100 {
		t.Errorf("expected priority 100, got %d", cmd.Priority)
	}
}

func TestParseCreateTrigger_AfterTiming(t *testing.T) {
	input := `CREATE TRIGGER "post_delete" ON BUNDLE "Items" AFTER DELETE FOR EACH DOCUMENT EXECUTE (ADD DOCUMENT TO BUNDLE "DeleteLog" WITH ({"deleted" = "true"}))`
	parser, err := NewTriggerParser(input)
	if err != nil {
		t.Fatalf("NewTriggerParser failed: %v", err)
	}

	cmd, err := parser.ParseCreateTrigger()
	if err != nil {
		t.Fatalf("ParseCreateTrigger failed: %v", err)
	}

	if cmd.Timing != models.TRIGGER_TIMING_AFTER {
		t.Errorf("expected AFTER timing, got %d", cmd.Timing)
	}
	if cmd.Events != models.TRIGGER_EVENT_DELETE {
		t.Errorf("expected DELETE event, got %d", cmd.Events)
	}
}

func TestParseCreateTrigger_MissingExecute(t *testing.T) {
	input := `CREATE TRIGGER "bad" ON BUNDLE "Orders" BEFORE INSERT FOR EACH DOCUMENT`
	parser, err := NewTriggerParser(input)
	if err != nil {
		t.Fatalf("NewTriggerParser failed: %v", err)
	}

	_, err = parser.ParseCreateTrigger()
	if err == nil {
		t.Error("expected error for missing EXECUTE clause")
	}
}

func TestParseCreateTrigger_InvalidTiming(t *testing.T) {
	input := `CREATE TRIGGER "bad" ON BUNDLE "Orders" DURING INSERT FOR EACH DOCUMENT EXECUTE (noop)`
	parser, err := NewTriggerParser(input)
	if err != nil {
		t.Fatalf("NewTriggerParser failed: %v", err)
	}

	_, err = parser.ParseCreateTrigger()
	if err == nil {
		t.Error("expected error for invalid timing keyword")
	}
}

func TestParseDropTrigger(t *testing.T) {
	input := `DROP TRIGGER "audit_insert" ON BUNDLE "Orders"`
	cmd, err := ParseDropTrigger(input)
	if err != nil {
		t.Fatalf("ParseDropTrigger failed: %v", err)
	}

	if cmd.TriggerName != "audit_insert" {
		t.Errorf("expected trigger name 'audit_insert', got %q", cmd.TriggerName)
	}
	if cmd.BundleName != "Orders" {
		t.Errorf("expected bundle name 'Orders', got %q", cmd.BundleName)
	}
}

func TestParseAlterTrigger_Enable(t *testing.T) {
	input := `ENABLE TRIGGER "my_trigger" ON BUNDLE "Products"`
	cmd, err := ParseAlterTrigger(input)
	if err != nil {
		t.Fatalf("ParseAlterTrigger failed: %v", err)
	}

	if cmd.TriggerName != "my_trigger" {
		t.Errorf("expected trigger name 'my_trigger', got %q", cmd.TriggerName)
	}
	if cmd.BundleName != "Products" {
		t.Errorf("expected bundle name 'Products', got %q", cmd.BundleName)
	}
	if !cmd.Enable {
		t.Error("expected Enable to be true")
	}
}

func TestParseAlterTrigger_Disable(t *testing.T) {
	input := `DISABLE TRIGGER "my_trigger" ON BUNDLE "Products"`
	cmd, err := ParseAlterTrigger(input)
	if err != nil {
		t.Fatalf("ParseAlterTrigger failed: %v", err)
	}

	if cmd.Enable {
		t.Error("expected Enable to be false")
	}
}

func TestParseCreateTrigger_WhenAndPriority(t *testing.T) {
	input := `CREATE TRIGGER "combo" ON BUNDLE "Data" BEFORE UPDATE FOR EACH DOCUMENT WHEN (NEW."value" > 100) PRIORITY 50 EXECUTE (ADD DOCUMENT TO BUNDLE "HighValues" WITH ({"alert" = "true"}))`
	parser, err := NewTriggerParser(input)
	if err != nil {
		t.Fatalf("NewTriggerParser failed: %v", err)
	}

	cmd, err := parser.ParseCreateTrigger()
	if err != nil {
		t.Fatalf("ParseCreateTrigger failed: %v", err)
	}

	if cmd.WhenClause == "" {
		t.Error("expected non-empty WHEN clause")
	}
	if cmd.Priority != 50 {
		t.Errorf("expected priority 50, got %d", cmd.Priority)
	}
	if cmd.Timing != models.TRIGGER_TIMING_BEFORE {
		t.Errorf("expected BEFORE timing")
	}
}

func TestTriggerDefinition_MatchesEvent(t *testing.T) {
	trig := models.TriggerDefinition{
		Enabled: true,
		Events:  models.TRIGGER_EVENT_INSERT | models.TRIGGER_EVENT_UPDATE,
	}

	if !trig.MatchesEvent(models.TRIGGER_EVENT_INSERT) {
		t.Error("expected trigger to match INSERT")
	}
	if !trig.MatchesEvent(models.TRIGGER_EVENT_UPDATE) {
		t.Error("expected trigger to match UPDATE")
	}
	if trig.MatchesEvent(models.TRIGGER_EVENT_DELETE) {
		t.Error("expected trigger NOT to match DELETE")
	}

	// Disabled trigger should not match
	trig.Enabled = false
	if trig.MatchesEvent(models.TRIGGER_EVENT_INSERT) {
		t.Error("disabled trigger should not match any event")
	}
}

func TestParseCreateTrigger_InsertUpdate(t *testing.T) {
	input := `CREATE TRIGGER "multi" ON BUNDLE "Items" AFTER INSERT, UPDATE FOR EACH DOCUMENT EXECUTE (ADD DOCUMENT TO BUNDLE "Log" WITH ({"x" = "y"}))`
	parser, err := NewTriggerParser(input)
	if err != nil {
		t.Fatalf("NewTriggerParser failed: %v", err)
	}

	cmd, err := parser.ParseCreateTrigger()
	if err != nil {
		t.Fatalf("ParseCreateTrigger failed: %v", err)
	}

	expected := models.TRIGGER_EVENT_INSERT | models.TRIGGER_EVENT_UPDATE
	if cmd.Events != expected {
		t.Errorf("expected INSERT|UPDATE events (bitmask %d), got %d", expected, cmd.Events)
	}
}
