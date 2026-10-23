package services

import "testing"

func TestValidateRejectsDependencyCycle(t *testing.T) {
	err := Validate([]Spec{{Name: "orders", Command: "go", Port: 8081, Dependencies: []string{"delivery"}}, {Name: "delivery", Command: "go", Port: 8082, Dependencies: []string{"orders"}}})
	if err == nil {
		t.Fatal("expected dependency cycle to be rejected")
	}
}

func TestBackoffIsBounded(t *testing.T) {
	policy := RestartPolicy{InitialBackoffMS: 500}
	if got, want := backoff(policy, 1).Milliseconds(), int64(500); got != want {
		t.Fatalf("first backoff = %d, want %d", got, want)
	}
	if got, want := backoff(policy, 10).Milliseconds(), int64(32000); got != want {
		t.Fatalf("bounded backoff = %d, want %d", got, want)
	}
}
