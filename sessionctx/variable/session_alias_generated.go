package variable

import (
	"strconv"

	"github.com/pingcap/tidb/sessionctx/variable/proto"
)

var _ SessionSysVarAliasReaderMutator = &SessionSysVarAlias{}
var _ aliasInnerOperator = &SessionSysVarAlias{}

type SessionSysVarAliasReaderMutator interface {
	SessionSysVarAliasReader
	SessionSysVarAliasMutator
}

type SessionSysVarAliasReader interface {
	GetMaxChunkSize() int
	GetDMLBatchSize() int
	GetSkipUTF8Check() bool
	GetBatchInsert() bool
	GetMPPStoreFailTTL() string
}

type SessionSysVarAliasMutator interface {
	SetMaxChunkSize(int)
	SetDMLBatchSize(int)
	SetSkipUTF8Check(bool)
}

type aliasInnerOperator interface {
	SetMaxChunkSizeWithVarStr(string) error
	GetMaxChunkSizeAsVarStr() (string, error)
	SetDMLBatchSizeWithVarStr(string) error
	GetDMLBatchSizeAsVarStr() (string, error)
	SetSkipUTF8CheckWithVarStr(string) error
	GetSkipUTF8CheckAsVarStr() (string, error)
	SetBatchInsertWithVarStr(string) error
	GetBatchInsertAsVarStr() (string, error)
	SetMPPStoreFailTTLWithVarStr(string) error
	GetMPPStoreFailTTLAsVarStr() (string, error)
}

type SessionSysVarAlias proto.SessionSystemVarDefs

func (s *SessionSysVarAlias) clone() *SessionSysVarAlias {
	var cloned SessionSysVarAlias
	cloned = *s
	return &cloned
}

func (s *SessionSysVarAlias) GetMaxChunkSize() int {
	return s.MaxChunkSize
}

func (s *SessionSysVarAlias) SetMaxChunkSize(val int) {
	s.MaxChunkSize = val
}

func (s *SessionSysVarAlias) SetMaxChunkSizeWithVarStr(val string) error {
	s.MaxChunkSize = TidbOptInt(val, DefMaxChunkSize)
	return nil
}

func (s *SessionSysVarAlias) GetMaxChunkSizeAsVarStr() (string, error) {
	return strconv.Itoa(s.MaxChunkSize), nil
}

func (s *SessionSysVarAlias) GetDMLBatchSize() int {
	return s.DMLBatchSize
}

func (s *SessionSysVarAlias) SetDMLBatchSize(val int) {
	s.DMLBatchSize = val
}

func (s *SessionSysVarAlias) SetDMLBatchSizeWithVarStr(val string) error {
	s.DMLBatchSize = TidbOptInt(val, DefDMLBatchSize)
	return nil
}

func (s *SessionSysVarAlias) GetDMLBatchSizeAsVarStr() (string, error) {
	return strconv.Itoa(s.DMLBatchSize), nil
}

func (s *SessionSysVarAlias) GetSkipUTF8Check() bool {
	return s.SkipUTF8Check
}

func (s *SessionSysVarAlias) SetSkipUTF8Check(val bool) {
	s.SkipUTF8Check = val
}

func (s *SessionSysVarAlias) SetSkipUTF8CheckWithVarStr(val string) error {
	s.SkipUTF8Check = TiDBOptOn(val)
	return nil
}

func (s *SessionSysVarAlias) GetSkipUTF8CheckAsVarStr() (string, error) {
	return BoolToOnOff(s.SkipUTF8Check), nil
}

func (s *SessionSysVarAlias) GetBatchInsert() bool {
	return s.BatchInsert
}

func (s *SessionSysVarAlias) SetBatchInsertWithVarStr(val string) error {
	s.BatchInsert = TiDBOptOn(val)
	return nil
}

func (s *SessionSysVarAlias) GetBatchInsertAsVarStr() (string, error) {
	return BoolToOnOff(s.BatchInsert), nil
}

func (s *SessionSysVarAlias) GetMPPStoreFailTTL() string {
	return s.MPPStoreFailTTL
}

func (s *SessionSysVarAlias) SetMPPStoreFailTTLWithVarStr(val string) error {
	s.MPPStoreFailTTL = val
	return nil
}

func (s *SessionSysVarAlias) GetMPPStoreFailTTLAsVarStr() (string, error) {
	return s.MPPStoreFailTTL, nil
}
