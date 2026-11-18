package policy

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"google.golang.org/genai"
)

func hideBound(declaration *genai.FunctionDeclaration, bound map[string]string) (*genai.FunctionDeclaration, error) {
	if len(bound) == 0 {
		return declaration, nil
	}
	raw, err := json.Marshal(declaration.ParametersJsonSchema)
	if err != nil {
		return nil, fmt.Errorf("encode %s parameters: %w", declaration.Name, err)
	}
	var schema map[string]any
	if err := json.Unmarshal(raw, &schema); err != nil || schema == nil {
		return nil, fmt.Errorf("decode %s parameters", declaration.Name)
	}
	for path := range bound {
		if !removeProperty(schema, schema, path) {
			return nil, fmt.Errorf("%s declares no bound argument %s", declaration.Name, path)
		}
	}
	hidden := *declaration
	hidden.ParametersJsonSchema = schema
	return &hidden, nil
}

func removeProperty(root, node map[string]any, path string) bool {
	node = resolveRef(root, node)
	properties, _ := node["properties"].(map[string]any)
	key, nested, ok := strings.Cut(path, ".")
	if ok {
		child, _ := properties[key].(map[string]any)
		return child != nil && removeProperty(root, child, nested)
	}
	if _, declared := properties[key]; !declared {
		return false
	}
	delete(properties, key)
	if required, ok := node["required"].([]any); ok {
		node["required"] = slices.DeleteFunc(required, func(name any) bool { return name == key })
	}
	return true
}

func resolveRef(root, node map[string]any) map[string]any {
	ref, _ := node["$ref"].(string)
	name, ok := strings.CutPrefix(ref, "#/$defs/")
	if !ok {
		return node
	}
	definitions, _ := root["$defs"].(map[string]any)
	target, ok := definitions[name].(map[string]any)
	if !ok {
		return node
	}
	return resolveRef(root, target)
}

func bindArgument(args map[string]any, path, value string) bool {
	key, nested, ok := strings.Cut(path, ".")
	if !ok {
		args[key] = value
		return true
	}
	object, ok := args[key].(map[string]any)
	if !ok {
		text, _ := args[key].(string)
		if json.Unmarshal([]byte(text), &object) != nil || object == nil {
			return false
		}
		args[key] = object
	}
	return bindArgument(object, nested, value)
}
