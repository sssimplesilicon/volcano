package workerscale

import (
	"context"
	"encoding/json"
	"fmt"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"strings"
)

// JournalEmpty requires a valid, explicit scheduler journal before ordinary scale-down.
// Missing/unreadable/malformed journals cannot establish that eviction has drained.
func JournalEmpty(ctx context.Context, c kubernetes.Interface, reference string) error {
	parts := strings.Split(reference, "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return fmt.Errorf("downscale requires a configured recovery journal")
	}
	cm, err := c.CoreV1().ConfigMaps(parts[0]).Get(ctx, parts[1], meta.GetOptions{})
	if err != nil {
		return err
	}
	if cm.Labels["scheduling.simplesilicon.com/recovery-journal"] != "v1" {
		return fmt.Errorf("unrecognized recovery journal")
	}
	var entries map[string]json.RawMessage
	if err = json.Unmarshal([]byte(cm.Data["intents.json"]), &entries); err != nil || entries == nil {
		return fmt.Errorf("invalid recovery journal")
	}
	if len(entries) > 0 {
		return fmt.Errorf("downscale deferred: outstanding eviction recovery intents")
	}
	return nil
}
