package errors

import (
	"fmt"
	"net/http"
)

type AppError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Details string `json:"details,omitempty"`
}

func (e *AppError) Error() string {
	return e.Message
}

func NewBadRequestError(message string) *AppError {
	return &AppError{
		Code:    http.StatusBadRequest,
		Message: message,
	}
}

func NewNotFoundError(message string) *AppError {
	return &AppError{
		Code:    http.StatusNotFound,
		Message: message,
	}
}

func NewInternalServerError(message string) *AppError {
	return &AppError{
		Code:    http.StatusInternalServerError,
		Message: message,
	}
}

func NewValidationError(message string, details string) *AppError {
	return &AppError{
		Code:    http.StatusBadRequest,
		Message: message,
		Details: details,
	}
}

func NewAppError(code int, errorCode string, message string) *AppError {
	return &AppError{
		Code:    code,
		Message: message,
		Details: errorCode,
	}
}

func WrapError(err error, message string) error {
	return fmt.Errorf("%s: %w", message, err)
}

