package core

import (
	"context"
	"errors"
	"io"
	"reflect"
	"testing"
	"time"
)

type mockRepository struct {
	loadContainerRowsFn       func(ctx context.Context) ([]ContainerRow, error)
	loadSupportingResourcesFn func(ctx context.Context) (Snapshot, error)
	loadContainerMetricsFn    func(ctx context.Context, rows []ContainerRow) (map[string]ContainerMetrics, float64, uint64, error)
	loadContainerLiveDataFn   func(ctx context.Context, containerID string, previousCPU, previousMem []float64, tail int) (ContainerLiveData, error)
	startContainerFn          func(ctx context.Context, id string) error
	stopContainerFn           func(ctx context.Context, id string) error
	restartContainerFn        func(ctx context.Context, id string) error

	calls []string
}

func (m *mockRepository) LoadContainerRows(ctx context.Context) ([]ContainerRow, error) {
	m.calls = append(m.calls, "rows")
	if m.loadContainerRowsFn != nil {
		return m.loadContainerRowsFn(ctx)
	}
	return nil, nil
}

func (m *mockRepository) LoadSupportingResources(ctx context.Context) (Snapshot, error) {
	m.calls = append(m.calls, "resources")
	if m.loadSupportingResourcesFn != nil {
		return m.loadSupportingResourcesFn(ctx)
	}
	return Snapshot{}, nil
}

func (m *mockRepository) LoadContainerMetrics(ctx context.Context, rows []ContainerRow) (map[string]ContainerMetrics, float64, uint64, error) {
	m.calls = append(m.calls, "metrics")
	if m.loadContainerMetricsFn != nil {
		return m.loadContainerMetricsFn(ctx, rows)
	}
	return nil, 0, 0, nil
}

func (m *mockRepository) LoadContainerLiveData(ctx context.Context, containerID string, previousCPU, previousMem []float64, tail int) (ContainerLiveData, error) {
	m.calls = append(m.calls, "live")
	if m.loadContainerLiveDataFn != nil {
		return m.loadContainerLiveDataFn(ctx, containerID, previousCPU, previousMem, tail)
	}
	return ContainerLiveData{}, nil
}

func (m *mockRepository) ExecShell(_ context.Context, _ string, _ io.Reader, _, _ io.Writer) error {
	return nil
}

func (m *mockRepository) StartContainer(ctx context.Context, id string) error {
	m.calls = append(m.calls, "start:"+id)
	if m.startContainerFn != nil {
		return m.startContainerFn(ctx, id)
	}
	return nil
}

func (m *mockRepository) StopContainer(ctx context.Context, id string) error {
	m.calls = append(m.calls, "stop:"+id)
	if m.stopContainerFn != nil {
		return m.stopContainerFn(ctx, id)
	}
	return nil
}

func (m *mockRepository) RestartContainer(ctx context.Context, id string) error {
	m.calls = append(m.calls, "restart:"+id)
	if m.restartContainerFn != nil {
		return m.restartContainerFn(ctx, id)
	}
	return nil
}

func TestServiceLoadSnapshot_ComposesDataAndMetrics(t *testing.T) {
	rows := []ContainerRow{{FullID: "id-1", Name: "one"}, {FullID: "id-2", Name: "two"}}
	metrics := map[string]ContainerMetrics{
		"id-1": {
			CPUPercent:       10.5,
			MemoryPercent:    33.0,
			MemoryUsage:      "512 MiB",
			MemoryLimit:      "2.0 GiB",
			MemoryUsageBytes: 512,
			MemoryLimitBytes: 2048,
		},
	}
	resources := Snapshot{
		Images:   []ImageRow{{ID: "img"}},
		Networks: []NetworkRow{{Name: "net"}},
		Volumes:  []VolumeRow{{Name: "vol"}},
	}

	repo := &mockRepository{}
	repo.loadContainerRowsFn = func(ctx context.Context) ([]ContainerRow, error) {
		if _, ok := ctx.Deadline(); !ok {
			t.Fatalf("LoadContainerRows context should have a deadline")
		}
		return rows, nil
	}
	repo.loadSupportingResourcesFn = func(ctx context.Context) (Snapshot, error) {
		return resources, nil
	}
	repo.loadContainerMetricsFn = func(ctx context.Context, gotRows []ContainerRow) (map[string]ContainerMetrics, float64, uint64, error) {
		if !reflect.DeepEqual(gotRows, rows) {
			t.Fatalf("LoadContainerMetrics rows = %#v, want %#v", gotRows, rows)
		}
		return metrics, 99.9, 12345, nil
	}

	svc := NewService(repo)
	snapshot, err := svc.LoadSnapshot()
	if err != nil {
		t.Fatalf("LoadSnapshot() error = %v, want nil", err)
	}

	if !reflect.DeepEqual(repo.calls, []string{"rows", "resources", "metrics"}) {
		t.Fatalf("repository call order = %#v, want [rows resources metrics]", repo.calls)
	}

	if len(snapshot.Containers) != 2 {
		t.Fatalf("snapshot.Containers len = %d, want 2", len(snapshot.Containers))
	}
	if snapshot.Containers[0].CPUPercent != 10.5 {
		t.Fatalf("snapshot container CPU = %v, want 10.5", snapshot.Containers[0].CPUPercent)
	}
	if snapshot.Containers[1].CPUPercent != 0 {
		t.Fatalf("snapshot container without metrics should remain unchanged")
	}
	if snapshot.TotalCPU != 99.9 || snapshot.TotalMem != 12345 {
		t.Fatalf("snapshot totals = (%v, %v), want (99.9, 12345)", snapshot.TotalCPU, snapshot.TotalMem)
	}
	if snapshot.Timestamp.IsZero() {
		t.Fatalf("snapshot timestamp should be populated")
	}
}

func TestServiceLoadSnapshot_StopsOnResourceError(t *testing.T) {
	repo := &mockRepository{}
	repo.loadContainerRowsFn = func(ctx context.Context) ([]ContainerRow, error) {
		return []ContainerRow{{FullID: "id-1"}}, nil
	}
	repo.loadSupportingResourcesFn = func(ctx context.Context) (Snapshot, error) {
		return Snapshot{}, errors.New("boom")
	}
	repo.loadContainerMetricsFn = func(ctx context.Context, rows []ContainerRow) (map[string]ContainerMetrics, float64, uint64, error) {
		t.Fatalf("LoadContainerMetrics should not be called after resource failure")
		return nil, 0, 0, nil
	}

	svc := NewService(repo)
	_, err := svc.LoadSnapshot()
	if err == nil {
		t.Fatalf("LoadSnapshot() error = nil, want non-nil")
	}
}

func TestServiceLoadContainerLiveData_UsesTailDependentTimeout(t *testing.T) {
	tests := []struct {
		name string
		tail int
		want time.Duration
	}{
		{name: "default timeout", tail: 100, want: 5 * time.Second},
		{name: "medium tail timeout", tail: 600, want: 20 * time.Second},
		{name: "all logs timeout", tail: 0, want: 60 * time.Second},
		{name: "large tail timeout", tail: 5000, want: 60 * time.Second},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gotDuration time.Duration
			repo := &mockRepository{}
			repo.loadContainerLiveDataFn = func(ctx context.Context, containerID string, previousCPU, previousMem []float64, tail int) (ContainerLiveData, error) {
				deadline, ok := ctx.Deadline()
				if !ok {
					t.Fatalf("LoadContainerLiveData context should have a deadline")
				}
				gotDuration = time.Until(deadline)
				return ContainerLiveData{ContainerID: containerID}, nil
			}

			svc := NewService(repo)
			_, err := svc.LoadContainerLiveData("id-1", nil, nil, tt.tail)
			if err != nil {
				t.Fatalf("LoadContainerLiveData() error = %v, want nil", err)
			}

			assertDurationApprox(t, gotDuration, tt.want, 2*time.Second)
		})
	}
}

func TestServiceLoadContainerLiveData_UsesConfiguredTimeouts(t *testing.T) {
	config := ServiceConfig{
		RequestTimeout:              3 * time.Second,
		LiveDataMediumTailThreshold: 50,
		LiveDataMediumTailTimeout:   7 * time.Second,
		LiveDataLargeTailThreshold:  100,
		LiveDataLargeTailTimeout:    11 * time.Second,
	}

	tests := []struct {
		name string
		tail int
		want time.Duration
	}{
		{name: "uses configured default timeout", tail: 10, want: 3 * time.Second},
		{name: "uses configured medium timeout", tail: 60, want: 7 * time.Second},
		{name: "uses configured large timeout for tail all", tail: 0, want: 11 * time.Second},
		{name: "uses configured large timeout over large threshold", tail: 200, want: 11 * time.Second},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gotDuration time.Duration
			repo := &mockRepository{}
			repo.loadContainerLiveDataFn = func(ctx context.Context, containerID string, previousCPU, previousMem []float64, tail int) (ContainerLiveData, error) {
				deadline, ok := ctx.Deadline()
				if !ok {
					t.Fatalf("LoadContainerLiveData context should have a deadline")
				}
				gotDuration = time.Until(deadline)
				return ContainerLiveData{ContainerID: containerID}, nil
			}

			svc := NewServiceWithConfig(repo, config)
			_, err := svc.LoadContainerLiveData("id-1", nil, nil, tt.tail)
			if err != nil {
				t.Fatalf("LoadContainerLiveData() error = %v, want nil", err)
			}

			assertDurationApprox(t, gotDuration, tt.want, 2*time.Second)
		})
	}
}

func TestNewServiceWithConfig_ZeroValuesUseDefaults(t *testing.T) {
	var gotDuration time.Duration
	repo := &mockRepository{}
	repo.loadContainerLiveDataFn = func(ctx context.Context, containerID string, previousCPU, previousMem []float64, tail int) (ContainerLiveData, error) {
		deadline, ok := ctx.Deadline()
		if !ok {
			t.Fatalf("LoadContainerLiveData context should have a deadline")
		}
		gotDuration = time.Until(deadline)
		return ContainerLiveData{ContainerID: containerID}, nil
	}

	svc := NewServiceWithConfig(repo, ServiceConfig{})
	_, err := svc.LoadContainerLiveData("id-1", nil, nil, 100)
	if err != nil {
		t.Fatalf("LoadContainerLiveData() error = %v, want nil", err)
	}

	assertDurationApprox(t, gotDuration, 5*time.Second, 2*time.Second)
}

func assertDurationApprox(t *testing.T, got, want, tolerance time.Duration) {
	t.Helper()
	min := want - tolerance
	max := want + tolerance
	if got < min || got > max {
		t.Fatalf("duration = %v, want within [%v, %v]", got, min, max)
	}
}

func TestService_StartContainer_CallsRepoWithID(t *testing.T) {
	repo := &mockRepository{}
	svc := NewService(repo)

	err := svc.StartContainer("abc123")

	if err != nil {
		t.Fatalf("StartContainer() error = %v, want nil", err)
	}
	if len(repo.calls) != 1 || repo.calls[0] != "start:abc123" {
		t.Fatalf("repo.calls = %v, want [start:abc123]", repo.calls)
	}
}

func TestService_StartContainer_PropagatesError(t *testing.T) {
	repo := &mockRepository{startContainerFn: func(_ context.Context, _ string) error {
		return errors.New("daemon not reachable")
	}}
	svc := NewService(repo)

	err := svc.StartContainer("abc123")

	if err == nil {
		t.Fatal("StartContainer() error = nil, want non-nil")
	}
}

func TestService_StopContainer_CallsRepoWithID(t *testing.T) {
	repo := &mockRepository{}
	svc := NewService(repo)

	err := svc.StopContainer("def456")

	if err != nil {
		t.Fatalf("StopContainer() error = %v, want nil", err)
	}
	if len(repo.calls) != 1 || repo.calls[0] != "stop:def456" {
		t.Fatalf("repo.calls = %v, want [stop:def456]", repo.calls)
	}
}

func TestService_StopContainer_PropagatesError(t *testing.T) {
	repo := &mockRepository{stopContainerFn: func(_ context.Context, _ string) error {
		return errors.New("container not found")
	}}
	svc := NewService(repo)

	err := svc.StopContainer("def456")

	if err == nil {
		t.Fatal("StopContainer() error = nil, want non-nil")
	}
}

func TestService_RestartContainer_CallsRepoWithID(t *testing.T) {
	repo := &mockRepository{}
	svc := NewService(repo)

	err := svc.RestartContainer("ghi789")

	if err != nil {
		t.Fatalf("RestartContainer() error = %v, want nil", err)
	}
	if len(repo.calls) != 1 || repo.calls[0] != "restart:ghi789" {
		t.Fatalf("repo.calls = %v, want [restart:ghi789]", repo.calls)
	}
}

func TestService_RestartContainer_PropagatesError(t *testing.T) {
	repo := &mockRepository{restartContainerFn: func(_ context.Context, _ string) error {
		return errors.New("restart failed")
	}}
	svc := NewService(repo)

	err := svc.RestartContainer("ghi789")

	if err == nil {
		t.Fatal("RestartContainer() error = nil, want non-nil")
	}
}
