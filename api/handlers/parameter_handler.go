package handlers

import (
	"fmt"
	"net/http"
	"strconv"

	"aoi-backend/internal/core/errors"
	"aoi-backend/internal/domain"
	"aoi-backend/internal/service"
	"github.com/gin-gonic/gin"
)

type ParameterHandler struct {
	service service.ParameterService
}

func NewParameterHandler(service service.ParameterService) *ParameterHandler {
	return &ParameterHandler{
		service: service,
	}
}

func (h *ParameterHandler) GetAll(c *gin.Context) {
	params, err := h.service.GetAll()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	// Debug: log the number of parameters retrieved
	fmt.Printf("GetAll: Retrieved %d parameters\n", len(params))

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"data":    params,
		"meta": gin.H{
			"total": len(params),
		},
	})
}

func (h *ParameterHandler) GetByID(c *gin.Context) {
	id, err := strconv.ParseUint(c.Param("id"), 10, 32)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   "Invalid parameter ID",
		})
		return
	}

	param, err := h.service.GetByID(uint(id))
	if err != nil {
		if appErr, ok := err.(*errors.AppError); ok {
			c.JSON(appErr.Code, gin.H{
				"success": false,
				"error":   appErr.Message,
			})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"data":    param,
	})
}

func (h *ParameterHandler) Create(c *gin.Context) {
	var param domain.Parameter
	if err := c.ShouldBindJSON(&param); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	if err := h.service.Create(&param); err != nil {
		if appErr, ok := err.(*errors.AppError); ok {
			c.JSON(appErr.Code, gin.H{
				"success": false,
				"error":   appErr.Message,
			})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"success": true,
		"data":    param,
	})
}

func (h *ParameterHandler) Update(c *gin.Context) {
	id, err := strconv.ParseUint(c.Param("id"), 10, 32)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   "Invalid parameter ID",
		})
		return
	}

	var param domain.Parameter
	if err := c.ShouldBindJSON(&param); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	if err := h.service.Update(uint(id), &param); err != nil {
		if appErr, ok := err.(*errors.AppError); ok {
			c.JSON(appErr.Code, gin.H{
				"success": false,
				"error":   appErr.Message,
			})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"data":    param,
	})
}

func (h *ParameterHandler) Delete(c *gin.Context) {
	id, err := strconv.ParseUint(c.Param("id"), 10, 32)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   "Invalid parameter ID",
		})
		return
	}

	if err := h.service.Delete(uint(id)); err != nil {
		if appErr, ok := err.(*errors.AppError); ok {
			c.JSON(appErr.Code, gin.H{
				"success": false,
				"error":   appErr.Message,
			})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"message": "Parameter deleted successfully",
	})
}

