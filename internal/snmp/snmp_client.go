package snmp

import (
	"context"
	"fmt"
	"time"

	"github.com/gosnmp/gosnmp"
	"aoi-backend/internal/domain"
)

type SNMPClient struct {
	client *gosnmp.GoSNMP
}

func NewSNMPClient(config *domain.SNMPConfig) (*SNMPClient, error) {
	port := config.Port
	if port == 0 {
		port = 161 // default SNMP port
	}

	var version gosnmp.SnmpVersion
	switch config.Version {
	case "v1":
		version = gosnmp.Version1
	case "v2c":
		version = gosnmp.Version2c
	case "v3":
		version = gosnmp.Version3
	default:
		version = gosnmp.Version2c // default
	}

	client := &gosnmp.GoSNMP{
		Target:    config.IPAddress,
		Port:      uint16(port),
		Community: config.Community,
		Version:   version,
		Timeout:   time.Duration(5) * time.Second,
		Retries:   3,
	}

	if err := client.Connect(); err != nil {
		return nil, fmt.Errorf("failed to connect to SNMP: %w", err)
	}

	return &SNMPClient{
		client: client,
	}, nil
}

func (c *SNMPClient) Get(ctx context.Context, oid string) (interface{}, error) {
	result, err := c.client.Get([]string{oid})
	if err != nil {
		return nil, fmt.Errorf("failed to get SNMP value: %w", err)
	}

	if len(result.Variables) == 0 {
		return nil, fmt.Errorf("no SNMP variable returned")
	}

	variable := result.Variables[0]
	
	switch variable.Type {
	case gosnmp.Integer:
		return variable.Value.(int), nil
	case gosnmp.OctetString:
		return string(variable.Value.([]byte)), nil
	case gosnmp.Counter32, gosnmp.Gauge32, gosnmp.TimeTicks:
		return variable.Value.(uint), nil
	case gosnmp.Counter64:
		return variable.Value.(uint64), nil
	case gosnmp.Uinteger32:
		return variable.Value.(uint32), nil
	default:
		return variable.Value, nil
	}
}

func (c *SNMPClient) Close() error {
	if c.client != nil {
		c.client.Conn.Close()
	}
	return nil
}

