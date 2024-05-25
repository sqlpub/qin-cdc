package core

type Position interface {
	LoadPosition(name string) string
	Start()
	Update(v string) error
	Save() error
	Get() string
	Close()
}
