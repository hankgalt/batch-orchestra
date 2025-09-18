package domain

import (
	"fmt"
	"sort"
	"strings"
)

func GetFetchActivityName[T any](srcCfg SourceConfig[T]) string {
	return "fetch-next-" + srcCfg.Name() + "-batch-activity-alias"
}

func GetWriteActivityName[T any](sinkCfg SinkConfig[T]) string {
	return "write-next-" + sinkCfg.Name() + "-batch-activity-alias"
}

func GetSnapshotActivityName(snapCfg SnapshotConfig) string {
	return "snapshot-" + snapCfg.Name() + "-batch-activity-alias"
}

func GetBatchId(start, end string, prefix, suffix string) string {
	if prefix == "" && suffix == "" {
		return fmt.Sprintf("batch-%s-%s", start, end)
	}

	if prefix != "" && suffix != "" {
		return fmt.Sprintf("%s-%s-%s-%s", prefix, start, end, suffix)
	}

	if prefix != "" {
		return fmt.Sprintf("%s-%s-%s", prefix, start, end)
	}

	return fmt.Sprintf("%s-%s-%s", start, end, suffix)
}

// BuildTransformerWithRules creates a transformer function based on the provided headers and rules.
// The rules map defines how to transform each header:
//   - If a header maps to an empty string, it is used as it is.
//   - If a header maps to rename rule, it is renamed to target.
//   - If a header maps to a group rule,
//     the values of grouped headers are grouped as string and set to new target field.
//     Order determines position in grouped string. Original headers are ignored.
//   - If a header maps to a new field rule,
//     a new target field is added with the newField value.
//     Original header is used as it is.
//
// The transformer function takes a slice of string values (CSV record) and returns a map[string]any
// where keys are the transformed header names and values are the corresponding record values.
//
// Example usage:
//
//	 For headers:
//		headers := []string{"ENTITY_NUM", "PHYSICAL_CITY", "PHYSICAL_ADDRESS", "WORK_ADDRESS", "POSITION_TYPE", "STATUS"}
//
//	 And rules:
//		rules := map[string]Rule{
//		  "ENTITY_NUM": {Target: "ENTITY_ID"},        // rename
//		  "PHYSICAL_CITY": {Target: "ADDRESS", Group: true, Order: 2},      // group into ADDRESS as second token
//		  "PHYSICAL_ADDRESS": {Target: "ADDRESS", Group: true, Order: 1},    // group into ADDRESS as first token
//		  "WORK_ADDRESS": {Target: "WORK_ADDR"},      // rename into WORK_ADDR
//		  "POSITION_TYPE": {Target: "AGENT_TYPE", NewField: "Principal"}, // new Target field AGENT_TYPE with value Principal
//		}
//
//	The transformer function will then transform following CSV record:
//		["A5639", "New York", "123 Main St", "7648 Gotham St New York NY", "Manager", ""]
//
//	Into:
//		map[string]any{
//	  		"entity_id": "A5639",
//	  		"address": "123 Main St New York",
//	  		"work_addr": "7648 Gotham St New York NY",
//	  		"agent_type": "Principal",
//	  		"position_type": "Manager",
//	  		"status": "",
//		}
//
// allowing for flexible transformations of CSV records based on the provided headers and rules.
func BuildTransformerWithRules(headers []string, rules map[string]Rule) TransformerFunc {
	type colPlan struct {
		target   string
		group    bool
		newField string
		source   string
		order    int
		val      string
	}
	plan := make([]colPlan, len(headers))
	for i, h := range headers {
		if r, ok := rules[h]; ok && r.Target != "" {
			plan[i] = colPlan{source: h, target: r.Target, group: r.Group, newField: r.NewField, order: r.Order}
		} else {
			plan[i] = colPlan{source: h, target: h}
		}
	}

	return func(values []string) map[string]any {
		out := make(map[string]any, len(plan))
		// groupParts := map[string][]string{}
		groupParts := map[string][]colPlan{}
		n := len(values)

		for i := 0; i < len(plan) && i < n; i++ {
			var val string
			if i < n {
				val = strings.TrimSpace(values[i])
			} else {
				val = "" // ensure presence for non-grouped keys even if value slice is short
			}

			p := plan[i]
			if p.group {
				// Skip empty parts for grouped targets
				if val != "" {
					p.val = val // Store the value in the plan for grouping
					groupParts[p.target] = append(groupParts[p.target], p)
				}
			} else {
				// Always set the key, even if val == ""
				if p.newField != "" {
					out[strings.ToLower(p.target)] = p.newField
					out[strings.ToLower(p.source)] = val
				} else {
					out[strings.ToLower(p.target)] = val
				}
			}
		}

		// sort grouped parts by order
		for target, parts := range groupParts {
			// Sort parts by order
			if len(parts) > 1 {
				sort.Slice(parts, func(i, j int) bool {
					return parts[i].order < parts[j].order
				})
				// Materialize grouped fields (join non-empty parts with single spaces)
				var joinedParts []string
				for _, p := range parts {
					if p.val != "" {
						joinedParts = append(joinedParts, p.val)

					}
				}
				out[strings.ToLower(target)] = strings.TrimSpace(strings.Join(joinedParts, " "))
			}
		}

		return out
	}
}

func BuildBusinessModelTransformRules() map[string]Rule {
	return map[string]Rule{
		"ENTITY_NUM":                  {Target: "ENTITY_ID"},                                // rename to ENTITY_ID
		"FIRST_NAME":                  {Target: "NAME", Group: true, Order: 1},              // group into NAME
		"MIDDLE_NAME":                 {Target: "NAME", Group: true, Order: 2},              // group into NAME
		"LAST_NAME":                   {Target: "NAME", Group: true, Order: 3},              // group into NAME
		"PHYSICAL_ADDRESS":            {Target: "ADDRESS"},                                  // rename to ADDRESS
		"PHYSICAL_ADDRESS1":           {Target: "ADDRESS", Group: true, Order: 1},           // group into ADDRESS
		"PHYSICAL_ADDRESS2":           {Target: "ADDRESS", Group: true, Order: 2},           // group into ADDRESS
		"PHYSICAL_ADDRESS3":           {Target: "ADDRESS", Group: true, Order: 3},           // group into ADDRESS
		"PHYSICAL_CITY":               {Target: "ADDRESS", Group: true, Order: 4},           // group into ADDRESS
		"PHYSICAL_STATE":              {Target: "ADDRESS", Group: true, Order: 5},           // group into ADDRESS
		"PHYSICAL_POSTAL_CODE":        {Target: "ADDRESS", Group: true, Order: 6},           // group into ADDRESS
		"PHYSICAL_COUNTRY":            {Target: "ADDRESS", Group: true, Order: 7},           // group into ADDRESS
		"ADDRESS1":                    {Target: "ADDRESS", Group: true, Order: 1},           // group into ADDRESS
		"ADDRESS2":                    {Target: "ADDRESS", Group: true, Order: 2},           // group into ADDRESS
		"ADDRESS3":                    {Target: "ADDRESS", Group: true, Order: 3},           // group into ADDRESS
		"CITY":                        {Target: "ADDRESS", Group: true, Order: 4},           // group into ADDRESS
		"STATE":                       {Target: "ADDRESS", Group: true, Order: 5},           // group into ADDRESS
		"POSTAL_CODE":                 {Target: "ADDRESS", Group: true, Order: 6},           // group into ADDRESS
		"COUNTRY":                     {Target: "ADDRESS", Group: true, Order: 7},           // group into ADDRESS
		"PRINCIPAL_ADDRESS":           {Target: "PRINCIPAL_ADDRESS", Group: true, Order: 1}, // group into PRINCIPAL_ADDRESS
		"PRINCIPAL_ADDRESS1":          {Target: "PRINCIPAL_ADDRESS", Group: true, Order: 2}, // group into PRINCIPAL_ADDRESS
		"PRINCIPAL_ADDRESS2":          {Target: "PRINCIPAL_ADDRESS", Group: true, Order: 3}, // group into PRINCIPAL_ADDRESS
		"PRINCIPAL_CITY":              {Target: "PRINCIPAL_ADDRESS", Group: true, Order: 4}, // group into PRINCIPAL_ADDRESS
		"PRINCIPAL_STATE":             {Target: "PRINCIPAL_ADDRESS", Group: true, Order: 5}, // group into PRINCIPAL_ADDRESS
		"PRINCIPAL_POSTAL_CODE":       {Target: "PRINCIPAL_ADDRESS", Group: true, Order: 6}, // group into PRINCIPAL_ADDRESS
		"PRINCIPAL_COUNTRY":           {Target: "PRINCIPAL_ADDRESS", Group: true, Order: 7}, // group into PRINCIPAL_ADDRESS
		"MAILING_ADDRESS":             {Target: "MAILING_ADDRESS", Group: true, Order: 1},   // group into MAILING_ADDRESS
		"MAILING_ADDRESS1":            {Target: "MAILING_ADDRESS", Group: true, Order: 2},   // group into MAILING_ADDRESS
		"MAILING_ADDRESS2":            {Target: "MAILING_ADDRESS", Group: true, Order: 3},   // group into MAILING_ADDRESS
		"MAILING_ADDRESS3":            {Target: "MAILING_ADDRESS", Group: true, Order: 4},   // group into MAILING_ADDRESS
		"MAILING_CITY":                {Target: "MAILING_ADDRESS", Group: true, Order: 5},   // group into MAILING_ADDRESS
		"MAILING_STATE":               {Target: "MAILING_ADDRESS", Group: true, Order: 6},   // group into MAILING_ADDRESS
		"MAILING_POSTAL_CODE":         {Target: "MAILING_ADDRESS", Group: true, Order: 7},   // group into MAILING_ADDRESS
		"MAILING_COUNTRY":             {Target: "MAILING_ADDRESS", Group: true, Order: 8},   // group into MAILING_ADDRESS
		"PRINCIPAL_ADDRESS_IN_CA":     {Target: "ADDRESS_IN_CA", Group: true, Order: 1},     // group into ADDRESS_IN_CA
		"PRINCIPAL_ADDRESS1_IN_CA":    {Target: "ADDRESS_IN_CA", Group: true, Order: 2},     // group into ADDRESS_IN_CA
		"PRINCIPAL_ADDRESS2_IN_CA":    {Target: "ADDRESS_IN_CA", Group: true, Order: 3},     // group into ADDRESS_IN_CA
		"PRINCIPAL_CITY_IN_CA":        {Target: "ADDRESS_IN_CA", Group: true, Order: 4},     // group into ADDRESS_IN_CA
		"PRINCIPAL_STATE_IN_CA":       {Target: "ADDRESS_IN_CA", Group: true, Order: 5},     // group into ADDRESS_IN_CA
		"PRINCIPAL_POSTAL_CODE_IN_CA": {Target: "ADDRESS_IN_CA", Group: true, Order: 6},     // group into ADDRESS_IN_CA
		"PRINCIPAL_COUNTRY_IN_CA":     {Target: "ADDRESS_IN_CA", Group: true, Order: 7},     // group into ADDRESS_IN_CA
		"POSITION_TYPE":               {Target: "AGENT_TYPE", NewField: "Principal"},        // new Target field AGENT_TYPE with value Principal
	}
}
