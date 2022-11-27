package raft

import (
	"testing"
)

func TestInMemoryStore_StoreLogs(t *testing.T) {
	type args struct {
		logs []Log
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "store single log",
			args: args{
				[]Log{{
					Index:   1,
					Term:    1,
					Command: []byte("echo \"hello\""),
				}},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i := NewInMemoryLogStore()
			if err := i.StoreLogs(tt.args.logs); (err != nil) != tt.wantErr {
				t.Errorf("StoreLogs() error = %v, wantErr %v", err, tt.wantErr)
			}

			if i.highIndex != 1 {
				t.Errorf("fak")
			}
		})
	}
}
