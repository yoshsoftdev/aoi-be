package repository

import (
	"aoi-backend/internal/domain"
	"aoi-backend/internal/core/database"
	"gorm.io/gorm"
)

type ModbusRTURepository interface {
	Create(config *domain.ModbusRTU) error
	GetByID(id uint) (*domain.ModbusRTU, error)
	GetByParameterID(paramID uint) (*domain.ModbusRTU, error)
	GetAll() ([]domain.ModbusRTU, error)
	Update(config *domain.ModbusRTU) error
	Delete(id uint) error
}

type ModbusTCPRepository interface {
	Create(config *domain.ModbusTCP) error
	GetByID(id uint) (*domain.ModbusTCP, error)
	GetByParameterID(paramID uint) (*domain.ModbusTCP, error)
	GetAll() ([]domain.ModbusTCP, error)
	Update(config *domain.ModbusTCP) error
	Delete(id uint) error
}

type modbusRTURepository struct {
	db *gorm.DB
}

type modbusTCPRepository struct {
	db *gorm.DB
}

func NewModbusRTURepository() ModbusRTURepository {
	return &modbusRTURepository{
		db: database.GetDB(),
	}
}

func NewModbusTCPRepository() ModbusTCPRepository {
	return &modbusTCPRepository{
		db: database.GetDB(),
	}
}

// Modbus RTU
func (r *modbusRTURepository) Create(config *domain.ModbusRTU) error {
	return r.db.Create(config).Error
}

func (r *modbusRTURepository) GetByID(id uint) (*domain.ModbusRTU, error) {
	var config domain.ModbusRTU
	err := r.db.Preload("Parameter").First(&config, id).Error
	if err != nil {
		return nil, err
	}
	return &config, nil
}

func (r *modbusRTURepository) GetByParameterID(paramID uint) (*domain.ModbusRTU, error) {
	var config domain.ModbusRTU
	err := r.db.Preload("Parameter").Where("id_parameter = ?", paramID).First(&config).Error
	if err != nil {
		return nil, err
	}
	return &config, nil
}

func (r *modbusRTURepository) GetAll() ([]domain.ModbusRTU, error) {
	var configs []domain.ModbusRTU
	err := r.db.Preload("Parameter").Find(&configs).Error
	return configs, err
}

func (r *modbusRTURepository) Update(config *domain.ModbusRTU) error {
	return r.db.Save(config).Error
}

func (r *modbusRTURepository) Delete(id uint) error {
	return r.db.Delete(&domain.ModbusRTU{}, id).Error
}

// Modbus TCP
func (r *modbusTCPRepository) Create(config *domain.ModbusTCP) error {
	return r.db.Create(config).Error
}

func (r *modbusTCPRepository) GetByID(id uint) (*domain.ModbusTCP, error) {
	var config domain.ModbusTCP
	err := r.db.Preload("Parameter").First(&config, id).Error
	if err != nil {
		return nil, err
	}
	return &config, nil
}

func (r *modbusTCPRepository) GetByParameterID(paramID uint) (*domain.ModbusTCP, error) {
	var config domain.ModbusTCP
	err := r.db.Preload("Parameter").Where("id_parameter = ?", paramID).First(&config).Error
	if err != nil {
		return nil, err
	}
	return &config, nil
}

func (r *modbusTCPRepository) GetAll() ([]domain.ModbusTCP, error) {
	var configs []domain.ModbusTCP
	err := r.db.Preload("Parameter").Find(&configs).Error
	return configs, err
}

func (r *modbusTCPRepository) Update(config *domain.ModbusTCP) error {
	return r.db.Save(config).Error
}

func (r *modbusTCPRepository) Delete(id uint) error {
	return r.db.Delete(&domain.ModbusTCP{}, id).Error
}

