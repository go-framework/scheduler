package scheduler

import (
	"context"
)

// Get zap config template with context.
func GetZapConfigTemplate(ctx context.Context) []*Template {
	templates := []*Template{
		&Template{
			Name:        "Writes",
			Field:       "writes",
			Type:        "[]string",
			Must:        false,
			Description: "logger write outs.",
			Example:     "lumberjack",
		},
		&Template{
			Name:        "Level",
			Field:       "level",
			Type:        "string",
			Must:        false,
			Description: "logger level, value: debug, info, warn, error, dpanic, panic, and fatal.",
			Example:     "info",
		},
		&Template{
			Name:        "Development",
			Field:       "development",
			Type:        "boolean",
			Must:        false,
			Description: "logger development mode.",
			Example:     "false",
		},
		&Template{
			Name:        "Console",
			Field:       "console",
			Type:        "boolean",
			Must:        false,
			Description: "enable console logger.",
			Example:     "false",
		},
	}

	return templates
}
