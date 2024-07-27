package kafka

import (
	gokafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/mitchellh/mapstructure"
	"github.com/sqlpub/qin-cdc/config"
	"github.com/sqlpub/qin-cdc/metas"
	"sync"
)

type MetaPlugin struct {
	*config.KafkaConfig
	topics   map[string]*Topic
	producer *gokafka.Producer
	mu       sync.Mutex
}

type Topic struct {
	Name      string
	Partition int
}

func (m *MetaPlugin) Configure(conf map[string]interface{}) error {
	m.KafkaConfig = &config.KafkaConfig{}
	var target = conf["target"]
	if err := mapstructure.Decode(target, m.KafkaConfig); err != nil {
		return err
	}
	return nil
}

func (m *MetaPlugin) LoadMeta(routers []*metas.Router) (err error) {
	m.topics = make(map[string]*Topic)
	m.producer, err = getProducer(m.KafkaConfig)
	if err != nil {
		return err
	}
	for _, router := range routers {
		dmlTopic := router.DmlTopic
		if ok := m.topics[dmlTopic]; ok != nil {
			m.topics[dmlTopic].Name = dmlTopic
			metadata, err := m.producer.GetMetadata(&dmlTopic, false, 3000)
			if err != nil {
				return err
			}
			m.topics[dmlTopic].Partition = len(metadata.Topics[dmlTopic].Partitions)
		}
	}
	return nil
}

func (m *MetaPlugin) GetMeta(router *metas.Router) (topic interface{}, err error) {
	return m.Get(router.DmlTopic)
}

func (m *MetaPlugin) Get(topicName string) (topic *Topic, err error) {
	return m.topics[topicName], err
}

func (m *MetaPlugin) GetAll() map[string]*Topic {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.topics
}

func (m *MetaPlugin) Add(newTopic *Topic) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.topics[newTopic.Name] = newTopic
	return nil
}

func (m *MetaPlugin) Update(newTopic *Topic) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.topics[newTopic.Name] = newTopic
	return nil
}

func (m *MetaPlugin) Delete(topicName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.topics, topicName)
	return nil
}

func (m *MetaPlugin) Save() error {
	return nil
}

func (m *MetaPlugin) Close() {
	closeProducer(m.producer)
}
