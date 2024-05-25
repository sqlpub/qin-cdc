package core

type Input interface {
	NewInput(metas *Metas)
	Start(pos Position, in chan *Msg)
	Close()
}
