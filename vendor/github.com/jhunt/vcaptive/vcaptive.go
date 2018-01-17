package vcaptive

import (
	"encoding/json"
	"os"
	"strconv"
	"strings"
)

type Application struct {
	ID      string   `json:"application_id"`
	Name    string   `json:"application_name"`
	Version string   `json:"application_version"`
	URIs    []string `json:"application_uris"`
}

type Services map[string][]Instance

type Instance struct {
	Name           string      `json:"name"`
	Label          string      `json:"label"`
	Tags           []string    `json:"tags"`
	Plan           string      `json:"plan"`
	Credentials    Credentials `json:"credentials"`
	Provider       interface{} `json:"provider"`
	SyslogDrainURL interface{} `json:"syslog_drain_url"`
}

type Credentials map[string]interface{}

func ParseServices(interf interface{}) (Services, error) {
	var s string
	switch interf.(type) {
	case string:
		s = interf.(string)
	default:
		tem, err := json.Marshal(interf)
		if err != nil {
			os.Exit(1)
		}
		s = string(tem)
	}
	var ss Services
	return ss, json.Unmarshal([]byte(s), &ss)
}

func ParseApplication(s string) (Application, error) {
	var a Application
	return a, json.Unmarshal([]byte(s), &a)
}

func (ss Services) Tagged(tags ...string) (Instance, bool) {
	for _, list := range ss {
		for _, svc := range list {
			for _, have := range svc.Tags {
				for _, want := range tags {
					if have == want {
						return svc, true
					}
				}
			}
		}
	}
	return Instance{}, false
}

func (ss Services) WithCredentials(keys ...string) (Instance, bool) {
	for _, list := range ss {
		for _, svc := range list {
			found := true
			for _, want := range keys {
				if _, ok := svc.Get(want); !ok {
					found = false
					break
				}
			}
			if found {
				return svc, true
			}
		}
	}
	return Instance{}, false
}

func (inst Instance) Get(key string) (interface{}, bool) {
	var o interface{}

	o = inst.Credentials
	for _, p := range strings.Split(key, ".") {
		switch o.(type) {
		case Credentials:
			v, ok := o.(Credentials)[p]
			if !ok {
				return nil, false
			}
			o = v

		case map[string]interface{}:
			v, ok := o.(map[string]interface{})[p]
			if !ok {
				return nil, false
			}
			o = v

		case []interface{}:
			u, err := strconv.ParseUint(p, 10, 0)
			if err != nil {
				return nil, false
			}
			i := int(u)
			if i >= len(o.([]interface{})) {
				return nil, false
			}
			o = o.([]interface{})[i]

		default:
			return nil, false
		}
	}

	return o, true
}

func (inst Instance) GetString(key string) (string, bool) {
	v, ok := inst.Get(key)
	if !ok {
		return "", false
	}

	switch v.(type) {
	case string:
		return v.(string), true
	default:
		return "", false
	}
}

func (inst Instance) GetUint(key string) (uint, bool) {
	v, ok := inst.Get(key)
	if !ok {
		return 0, false
	}

	switch v.(type) {
	case int:
		return uint(v.(int)), true
	case int8:
		return uint(v.(int8)), true
	case int16:
		return uint(v.(int16)), true
	case int32:
		return uint(v.(int32)), true
	case int64:
		return uint(v.(int64)), true
	case uint:
		return uint(v.(int)), true
	case uint8:
		return uint(v.(int8)), true
	case uint16:
		return uint(v.(int16)), true
	case uint32:
		return uint(v.(int32)), true
	case uint64:
		return uint(v.(int64)), true
	case float32:
		return uint(v.(float32)), true
	case float64:
		return uint(v.(float64)), true
	default:
		return 0, false
	}
}
