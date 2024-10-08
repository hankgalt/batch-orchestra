package batch_orchestra

import "github.com/google/uuid"

// ApplicationName is the task list for batch task workflows
const ApplicationName = "batchTaskGroup"

// HostID - Use a new uuid just for demo so we can run 2 host specific activity workers on same machine.
// In real world case, you would use a hostname or ip address as HostID.
var HostID = ApplicationName + "_" + uuid.New().String()

// ProcessBatchRequestWorkflowName is the task list for processing a local file(csv) in batches
const ProcessBatchRequestWorkflowName = "github.com/hankgalt/batch-orchestra.ProcessBatchRequestWorkflow"
