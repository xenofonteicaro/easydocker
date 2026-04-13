package docker

import (
	"context"
	"errors"
	"testing"
)

func TestShellExecOptions(t *testing.T) {
	opts := shellExecOptions()
	if !opts.AttachStdin {
		t.Error("AttachStdin should be true")
	}
	if !opts.AttachStdout {
		t.Error("AttachStdout should be true")
	}
	if !opts.AttachStderr {
		t.Error("AttachStderr should be true")
	}
	if !opts.Tty {
		t.Error("Tty should be true")
	}
	if len(opts.Cmd) != 1 || opts.Cmd[0] != "sh" {
		t.Errorf("Cmd = %v, want [sh]", opts.Cmd)
	}
}

func TestExecShell_PropagatesClientError(t *testing.T) {
	r := &Repository{}
	r.clientOnce.Do(func() {
		r.clientErr = errors.New("no docker socket")
	})

	err := r.ExecShell(context.Background(), "ctr-1", nil, nil, nil)
	if err == nil {
		t.Fatal("ExecShell() should return error when client fails to initialize")
	}
	if err.Error() != "no docker socket" {
		t.Fatalf("ExecShell() error = %q, want 'no docker socket'", err.Error())
	}
}
