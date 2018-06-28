package my_drop_fields

import (
	"fmt"
	"strings"

	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/processors"
)

type dropFields struct {
	Fields []string
	log    *logp.Logger
}

func New(c *common.Config) (processors.Processor, error) {
	config := struct {
		Fields []string `config:"fields"`
	}{}
	err := c.Unpack(&config)
	if err != nil {
		return nil, fmt.Errorf("fail to unpack the drop_fields configuration: %s", err)
	}

	/* remove read only fields */
	for _, readOnly := range processors.MandatoryExportedFields {
		for i, field := range config.Fields {
			if readOnly == field {
				config.Fields = append(config.Fields[:i], config.Fields[i+1:]...)
			}
		}
	}

	f := dropFields{Fields: config.Fields, log: logp.NewLogger("mydropfields")}
	return f, nil
}

func (f dropFields) Run(event *beat.Event) (*beat.Event, error) {
	f.log.Info("Run my drop field")
	var errors []string

	for _, field := range f.Fields {
		err := event.Delete(field)
		if err != nil {
			errors = append(errors, err.Error())
		}
	}

	if len(errors) > 0 {
		return event, fmt.Errorf(strings.Join(errors, ", "))
	}
	return event, nil
}

func (f dropFields) String() string {
	return "my_drop_fields=" + strings.Join(f.Fields, ", ")
}
