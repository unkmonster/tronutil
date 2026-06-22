package abi

import (
	"fmt"
	"reflect"

	"github.com/ethereum/go-ethereum/accounts/abi"
	eCommon "github.com/ethereum/go-ethereum/common"
	"github.com/fbsobreira/gotron-sdk/pkg/address"
)

func DecodeInputDataBySelector(a abi.ABI, selector []byte, data []byte) (map[string]any, error) {
	method, err := a.MethodById(selector)
	if err != nil {
		return nil, fmt.Errorf("method by id %q: %w", string(selector), err)
	}

	var args abi.Arguments

	args = method.Inputs
	rv := make(map[string]any)
	err = args.UnpackIntoMap(rv, data[4:])
	if err != nil {
		return nil, fmt.Errorf("unpack args: %w", err)
	}

	for k, v := range rv {
		rv[k] = convertOutputValue(v)
	}
	return rv, nil
}

// convertOutputValue converts Ethereum addresses to TRON addresses in decoded
// ABI output values. Handles single addresses, address slices, and fixed-size
// address arrays (e.g. [3]common.Address). For unrecognized types, the value
// is returned unchanged.
func convertOutputValue(v interface{}) interface{} {
	switch val := v.(type) {
	case eCommon.Address:
		return ethToTronAddress(val)
	case []eCommon.Address:
		result := make([]address.Address, len(val))
		for i, addr := range val {
			result[i] = ethToTronAddress(addr)
		}
		return result
	default:
		// Handle fixed-size address arrays ([N]common.Address) which
		// go-ethereum returns for Solidity fixed-size array outputs.
		rv := reflect.ValueOf(v)
		if rv.Kind() == reflect.Array && rv.Type().Elem() == reflect.TypeOf(eCommon.Address{}) {
			result := make([]address.Address, rv.Len())
			for i := range result {
				result[i] = ethToTronAddress(rv.Index(i).Interface().(eCommon.Address))
			}
			return result
		}
		return v
	}
}

// ethToTronAddress converts an Ethereum common.Address to a TRON address
// by prepending the 0x41 prefix byte.
func ethToTronAddress(addr eCommon.Address) address.Address {
	tronAddr := make([]byte, 1+len(addr.Bytes()))
	tronAddr[0] = address.TronBytePrefix
	copy(tronAddr[1:], addr.Bytes())
	return address.Address(tronAddr)
}
