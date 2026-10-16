package options

import (
	"github.com/spf13/pflag"
	"testing"
)

func TestWorkerPreemptionDefaultsOff(t *testing.T) {
	o := NewServerOption()
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	o.AddFlags(fs)
	if o.EnableWorkerTypePreemption {
		t.Fatal("extension defaults on")
	}
	if err := fs.Parse([]string{"--enable-worker-type-preemption=true"}); err != nil {
		t.Fatal(err)
	}
	if !o.EnableWorkerTypePreemption {
		t.Fatal("explicit opt-in ignored")
	}
}
