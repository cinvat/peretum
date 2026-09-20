package base

func GetBool(config map[string]any, key string) bool {
	if v, ok := config[key].(bool); ok {
		return v
	}
	return false
}

func GetInt(config map[string]any, key string) int {
	if v, ok := config[key].(int); ok {
		return v
	}
	if v, ok := config[key].(float64); ok {
		return int(v)
	}
	return 0
}

func GetIntOrDefault(config map[string]any, key string, defaultVal int) int {
	if v, ok := config[key].(int); ok {
		return v
	}
	if v, ok := config[key].(float64); ok {
		return int(v)
	}
	return defaultVal
}

func GetInt64(config map[string]any, key string) int64 {
	if v, ok := config[key].(int64); ok {
		return v
	}
	if v, ok := config[key].(float64); ok {
		return int64(v)
	}
	if v, ok := config[key].(int); ok {
		return int64(v)
	}
	return 0
}

func GetString(config map[string]any, key string) string {
	if v, ok := config[key].(string); ok {
		return v
	}
	return ""
}

func GetStringSlice(config map[string]any, key string) []string {
	if v, ok := config[key].([]any); ok {
		result := make([]string, 0, len(v))
		for _, item := range v {
			if str, ok := item.(string); ok {
				result = append(result, str)
			}
		}
		return result
	}
	if v, ok := config[key].([]string); ok {
		return v
	}
	return nil
}
