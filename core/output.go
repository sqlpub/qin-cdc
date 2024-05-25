package core

type Output interface {
	NewOutput(metas *Metas)
	Start(out chan *Msg, pos Position)
	Close()
}
