// Deprecated binary name: retained for rolling upgrade compatibility.
// Standalone unified workload routing and node-pool admission server.
package main

import policy "volcano.sh/volcano/pkg/webhooks/admission/workloadpolicy"

func main() { policy.Run() }
