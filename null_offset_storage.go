package main

type NullOffsetStorage struct {
	*StartStopControl
}

func NewNullOffsetStorage() OffsetStorage {
	return &NullOffsetStorage{
		StartStopControl: NewStartStopControl(),
	}
}

func (nos *NullOffsetStorage) Init(config OffsetStorageConfig, manager *CallbackManager) error {
	return nil
}

func (nos *NullOffsetStorage) InitializePartition(topic string, partition int32) (int64, error) {
	return -1, nil
}

func (nos *NullOffsetStorage) FinalizePartition(topic string, partition int32) error {
	return nil
}

func (nos *NullOffsetStorage) ReadOffset(topic string, partition int32) (int64, error) {
	return -1, nil
}

func (nos *NullOffsetStorage) WriteOffset(topic string, partition int32, offset int64) error {
	return nil
}

func (nos *NullOffsetStorage) Run() error {
	if err := nos.ensureStart(); err != nil {
		return err
	}
	defer nos.markStop()

	select {
	case <-nos.WaitForCloseChannel():
	}

	return nil
}

func (nos *NullOffsetStorage) GetStat() interface{} {
	return make(map[string]interface{})
}

func init() {
	RegisterOffsetStorage("Null", NewNullOffsetStorage)
}
