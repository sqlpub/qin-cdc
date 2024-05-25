package transforms

func FindColumn(data map[string]interface{}, name string) interface{} {
	if value, ok := data[name]; ok {
		return value
	}
	return nil
}
