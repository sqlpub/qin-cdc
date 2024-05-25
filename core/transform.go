package core

type Transform interface {
	NewTransform(config map[string]interface{}) error
	Transform(msg *Msg) bool
}
