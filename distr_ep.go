package distr_ep

type DistributedEventProcessor struct {
	// TODO
	// name space details
	// redis connection
	// TTL for lock
}

func (d *DistributedEventProcessor) Init() {
	// TODO
	// Init all the resources
	// Start the key-stream consumer
	// Start the clean-up goroutine
}

func (d *DistributedEventProcessor) keyStreamConsumer() {
	// TODO
	// consume events from :key-stream stream
}

func (d *DistributedEventProcessor) AddEvent(key string, payload []byte) {
	// TODO
	// Add the element to {NS}:events:{key} stream
	// Add the element to {NS}:key-stream stream
}

func (d *DistributedEventProcessor) keyEventHandler(key string) {
	// TODO
	// Obtain EVENT_LOCK to {NS}:proc-lock:{key}, with TTL
	// If lock cant be obtained, return
	// Defer lock release
	// Start consuming messages from the events:{key} stream
	// when message is received, renew lock's TTL (to account for wait time)
	// process message synchronously
	// mark message as processed
}

func (d *DistributedEventProcessor) cleanup() {
	// TODO
}
