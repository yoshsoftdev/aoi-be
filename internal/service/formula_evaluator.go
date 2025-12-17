package service

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"

	"github.com/Knetic/govaluate"
)

type FormulaEvaluator struct{}

func NewFormulaEvaluator() *FormulaEvaluator {
	return &FormulaEvaluator{}
}

// Evaluate evaluates formula with a single value (backward compatibility)
func (e *FormulaEvaluator) Evaluate(formula string, value float64) (float64, error) {
	if formula == "" {
		return value, nil
	}

	// Create expression
	expression, err := govaluate.NewEvaluableExpression(formula)
	if err != nil {
		return 0, fmt.Errorf("invalid formula: %w", err)
	}

	// Prepare parameters
	parameters := make(map[string]interface{})
	parameters["value"] = value

	// Evaluate
	result, err := expression.Evaluate(parameters)
	if err != nil {
		return 0, fmt.Errorf("failed to evaluate formula: %w", err)
	}

	// Convert result to float64
	var resultFloat float64
	switch v := result.(type) {
	case float64:
		resultFloat = v
	case int:
		resultFloat = float64(v)
	case int64:
		resultFloat = float64(v)
	case string:
		parsed, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return 0, fmt.Errorf("formula result is not a number: %v", result)
		}
		resultFloat = parsed
	default:
		return 0, fmt.Errorf("formula result is not a number: %v", result)
	}

	// Round to 3 decimal places (using math.Round to avoid overflow for large numbers)
	resultFloat = math.Round(resultFloat*1000) / 1000

	return resultFloat, nil
}

// EvaluateWithFields evaluates formula with field names like "Slave 6 Addr 4 / 10"
// fieldValues is a map of field names to their values
func (e *FormulaEvaluator) EvaluateWithFields(formula string, fieldValues map[string]float64) (float64, error) {
	if formula == "" {
		return 0, fmt.Errorf("formula is empty")
	}

	// Round all field values to 3 decimal places (using math.Round to avoid overflow for large numbers)
	roundedValues := make(map[string]float64)
	for k, v := range fieldValues {
		roundedValues[k] = math.Round(v*1000) / 1000
	}

	// Replace field names with their values in the formula
	expression := formula
	for fieldName, fieldValue := range roundedValues {
		// Escape special regex characters in field name
		escapedName := regexp.QuoteMeta(fieldName)
		// Replace all occurrences of the field name with its value
		re := regexp.MustCompile(escapedName)
		expression = re.ReplaceAllString(expression, fmt.Sprintf("%.3f", fieldValue))
	}

	// Trim whitespace
	expression = strings.TrimSpace(expression)

	// Create evaluable expression
	evalExpr, err := govaluate.NewEvaluableExpression(expression)
	if err != nil {
		return 0, fmt.Errorf("invalid formula after field replacement: %w", err)
	}

	// Evaluate (no parameters needed since we already replaced field names)
	result, err := evalExpr.Evaluate(nil)
	if err != nil {
		return 0, fmt.Errorf("failed to evaluate formula: %w", err)
	}

	// Convert result to float64
	var resultFloat float64
	switch v := result.(type) {
	case float64:
		resultFloat = v
	case int:
		resultFloat = float64(v)
	case int64:
		resultFloat = float64(v)
	case string:
		parsed, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return 0, fmt.Errorf("formula result is not a number: %v", result)
		}
		resultFloat = parsed
	default:
		return 0, fmt.Errorf("formula result is not a number: %v", result)
	}

	// Round to 3 decimal places (using math.Round to avoid overflow for large numbers)
	resultFloat = math.Round(resultFloat*1000) / 1000

	return resultFloat, nil
}

