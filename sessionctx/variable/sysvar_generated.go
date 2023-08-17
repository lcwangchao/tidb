package variable

import "strconv"

const (
	TiDBMaxChunkSize = "tidb_max_chunk_size"
	TiDBDMLBatchSize = "tidb_dml_batch_size"
	TiDBSkipUTF8Check = "tidb_skip_utf8_check"
	TiDBBatchInsert = "tidb_batch_insert"
	TiDBMppStoreFailTtl = "tidb_mpp_store_fail_ttl"
)

const (
	DefMaxChunkSize int = 1024
	DefDMLBatchSize int = 0
	DefSkipUTF8Check bool = false
	DefBatchInsert bool = false
	DefMPPStoreFailTTL string = "60s"
)

var generatedSessionSysVars = []*SysVar{
	{
		Scope: ScopeGlobal | ScopeSession,
		Name: TiDBMaxChunkSize,
		Type: TypeUnsigned,
		Value: strconv.Itoa(DefMaxChunkSize),
		GetSession: func(s *SessionVars) (string, error) {
			return s.sysVarAlias.GetMaxChunkSizeAsVarStr()
		},
		SetSession: func(s *SessionVars, val string) error {
			return s.sysVarAlias.SetMaxChunkSizeWithVarStr(val)
		},
	},
	{
		Scope: ScopeGlobal | ScopeSession,
		Name: TiDBDMLBatchSize,
		Type: TypeUnsigned,
		Value: strconv.Itoa(DefDMLBatchSize),
		GetSession: func(s *SessionVars) (string, error) {
			return s.sysVarAlias.GetDMLBatchSizeAsVarStr()
		},
		SetSession: func(s *SessionVars, val string) error {
			return s.sysVarAlias.SetDMLBatchSizeWithVarStr(val)
		},
	},
	{
		Scope: ScopeGlobal | ScopeSession,
		Name: TiDBSkipUTF8Check,
		Type: TypeBool,
		Value: BoolToOnOff(DefSkipUTF8Check),
		GetSession: func(s *SessionVars) (string, error) {
			return s.sysVarAlias.GetSkipUTF8CheckAsVarStr()
		},
		SetSession: func(s *SessionVars, val string) error {
			return s.sysVarAlias.SetSkipUTF8CheckWithVarStr(val)
		},
	},
	{
		Scope: ScopeSession,
		Name: TiDBBatchInsert,
		Type: TypeBool,
		Value: BoolToOnOff(DefBatchInsert),
		SetSession: func(s *SessionVars, val string) error {
			return s.sysVarAlias.SetBatchInsertWithVarStr(val)
		},
	},
	{
		Scope: ScopeGlobal | ScopeSession,
		Name: TiDBMppStoreFailTtl,
		Type: TypeStr,
		Value: DefMPPStoreFailTTL,
		SetSession: func(s *SessionVars, val string) error {
			return s.sysVarAlias.SetMPPStoreFailTTLWithVarStr(val)
		},
	},
}
