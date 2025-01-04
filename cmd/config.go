package cmd

import (
	"os"
	"regexp"
	"strings"

	"github.com/exiledavatar/cmsrvu/cmsrvu"
	"gopkg.in/yaml.v3"
)

func LoadAndParseConfig(filename string) (*cmsrvu.Config, error) {
	// read config file
	b, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	b = ExpandEnvVars(b)
	cfg := cmsrvu.Config{}
	if err := yaml.Unmarshal(b, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// ExpandEnvVars substitutes environment variables of the form ${ENV_VAR_NAME}
// if you have characters that need to be escaped, they should be surrounded in
// quotes in the source string.
func ExpandEnvVars[T []byte | string](value T) T {
	s := string(value)

	re := regexp.MustCompile(`\$\{.+\}`)

	envvars := map[string]string{}
	for _, m := range re.FindAllString(s, -1) {
		mre := regexp.MustCompile(`[${}]`)
		mtrimmed := mre.ReplaceAllString(m, "")
		// fmt.Printf("%s:\t%s\n", mtrimmed, os.Getenv(mtrimmed))
		envvars[m] = os.Getenv(mtrimmed)
	}

	for k, v := range envvars {
		s = strings.ReplaceAll(s, k, v)
	}
	return T(s)
}
