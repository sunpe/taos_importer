package config

type Config struct {
	Title          string   `json:"title,omitempty" yaml:"title" toml:"title"`
	AutoCreate     bool     `json:"auto_create,omitempty" yaml:"auto_create" toml:"auto_create"`
	OutputFile     string   `json:"output_file,omitempty" yaml:"output_file" toml:"output_file"`
	DataDir        string   `json:"data_dir,omitempty" yaml:"data_dir" toml:"data_dir"`
	DataFileSuffix string   `json:"data_file_suffix,omitempty" yaml:"data_file_suffix" toml:"data_file_suffix"`
	DataFiles      []string `json:"data_files,omitempty" yaml:"data_files" toml:"data_files"`
	TagsDir        string   `json:"tags_dir,omitempty" yaml:"tags_dir" toml:"tags_dir"`
	TagsFileSuffix string   `json:"tags_file_suffix,omitempty" yaml:"tags_file_suffix" toml:"tags_file_suffix"`
	TagsFiles      []string `json:"tags_files,omitempty" yaml:"tags_files" toml:"tags_files"`
	BatchSize      int      `json:"batch_size,omitempty" yaml:"batch_size" toml:"batch_size"`
	Concurrent     int      `json:"concurrent" yaml:"concurrent" toml:"concurrent"`
	TDEngine       TDEngine `json:"tdengine" yaml:"tdengine" toml:"tdengine"`
	DB             Database `json:"db" yaml:"db" toml:"db"`
	STable         STable   `json:"stable" yaml:"stable" toml:"stable"`
}

type TDEngine struct {
	Host     string `json:"host,omitempty" yaml:"host" toml:"host"`
	Port     int    `json:"port,omitempty" yaml:"port" toml:"port"`
	User     string `json:"user,omitempty" yaml:"user" toml:"user"`
	Password string `json:"password,omitempty" yaml:"password" toml:"password"`
}

type Database struct {
	Name       string `json:"name,omitempty" yaml:"name" toml:"name"`
	Buffer     int    `json:"buffer,omitempty" yaml:"buffer" toml:"buffer"`
	CacheModel string `json:"cache_model,omitempty" yaml:"cache_model" toml:"cache_model"`
	CacheSize  int    `json:"cache_size,omitempty" yaml:"cache_size" toml:"cache_size"`
	Duration   string `json:"duration,omitempty" yaml:"duration" toml:"duration"`
	Keep       int    `json:"keep,omitempty" yaml:"keep" toml:"keep"`
	Precision  string `json:"precision,omitempty" yaml:"precision" toml:"precision"`
	VGroups    int    `json:"v_groups,omitempty" yaml:"v_groups" toml:"v_groups"`
}

type STable struct {
	Name                 string   `json:"name,omitempty" yaml:"name" toml:"name"`
	ChildTableNamePrefix string   `json:"child_table_name_prefix" yaml:"child_table_name_prefix" toml:"child_table_name_prefix"`
	ChildTableName       string   `json:"child_table_name,omitempty" yaml:"child_table_name" toml:"child_table_name"`
	Columns              []Column `json:"columns,omitempty" yaml:"columns" toml:"columns"`
	Tags                 []Column `json:"tags,omitempty" yaml:"tags" toml:"tags"`
}

type Column struct {
	Field  string `json:"field,omitempty" yaml:"field" toml:"field"`
	Type   string `json:"type,omitempty" yaml:"type" toml:"type"`
	Source string `json:"source,omitempty" yaml:"source" toml:"source"`
}
