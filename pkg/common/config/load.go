package config

import (
	"bytes"

	"github.com/spf13/viper"
)

type Parser[T any] func(v *viper.Viper) (*T, error)

func LoadConfigFromString[T any](cfg string, parser Parser[T]) (*T, error) {
	v := viper.New()
	v.SetConfigType("yaml")
	err := v.ReadConfig(bytes.NewBufferString(cfg))
	if err != nil {
		return nil, err
	}
	return parser(v)
}
