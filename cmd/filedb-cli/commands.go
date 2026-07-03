package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"

	"github.com/spf13/cobra"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/structpb"

	pb "github.com/srjn45/filedbv2/internal/pb/proto"
)

// ---- Collections ----------------------------------------------------------

func collectionsCmd(flags *cliFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "collections",
		Short: "List all collections",
		RunE: func(cmd *cobra.Command, _ []string) error {
			_, client, cleanup, err := connect(flags)
			if err != nil {
				return err
			}
			defer cleanup()

			resp, err := client.ListCollections(ctxWithAuth(flags), &pb.ListCollectionsRequest{})
			if err != nil {
				return err
			}
			for _, name := range resp.Names {
				_, _ = fmt.Fprintln(cmd.OutOrStdout(), name)
			}
			return nil
		},
	}
}

func createCollectionCmd(flags *cliFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "create-collection <name>",
		Short: "Create a new collection",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			_, client, cleanup, err := connect(flags)
			if err != nil {
				return err
			}
			defer cleanup()

			resp, err := client.CreateCollection(ctxWithAuth(flags), &pb.CreateCollectionRequest{Name: args[0]})
			if err != nil {
				return err
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "created collection %q at %s\n", resp.Name, resp.CreatedAt)
			return nil
		},
	}
}

func dropCollectionCmd(flags *cliFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "drop-collection <name>",
		Short: "Drop a collection and all its data",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			_, client, cleanup, err := connect(flags)
			if err != nil {
				return err
			}
			defer cleanup()

			_, err = client.DropCollection(ctxWithAuth(flags), &pb.DropCollectionRequest{Name: args[0]})
			if err != nil {
				return err
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "dropped %q\n", args[0])
			return nil
		},
	}
}

// ---- CRUD -----------------------------------------------------------------

func insertCmd(flags *cliFlags) *cobra.Command {
	var txID string
	cmd := &cobra.Command{
		Use:   "insert <collection> <json>",
		Short: "Insert a record",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			_, client, cleanup, err := connect(flags)
			if err != nil {
				return err
			}
			defer cleanup()

			data, err := parseJSONArg(args[1])
			if err != nil {
				return err
			}
			ctx := ctxWithAuth(flags)
			if txID != "" {
				ctx = ctxWithTx(flags, txID)
			}
			resp, err := client.Insert(ctx, &pb.InsertRequest{Collection: args[0], Data: data})
			if err != nil {
				return err
			}
			if txID != "" {
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "staged insert id:%d (tx:%s)\n", resp.Id, txID)
			} else {
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "inserted id:%d (%s)\n", resp.Id, resp.DateAdded)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&txID, "tx-id", "", "Stage this operation inside an open transaction")
	return cmd
}

func findCmd(flags *cliFlags) *cobra.Command {
	var (
		limit      uint32
		offset     uint32
		orderBy    string
		descending bool
	)
	cmd := &cobra.Command{
		Use:   "find <collection> [filter-json]",
		Short: "Find records matching a filter",
		Args:  cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			_, client, cleanup, err := connect(flags)
			if err != nil {
				return err
			}
			defer cleanup()

			req := &pb.FindRequest{
				Collection: args[0],
				Limit:      limit,
				Offset:     offset,
				OrderBy:    orderBy,
				Descending: descending,
			}

			if len(args) == 2 {
				req.Filter, err = parseFilterArg(args[1])
				if err != nil {
					return fmt.Errorf("filter: %w", err)
				}
			}

			stream, err := client.Find(ctxWithAuth(flags), req)
			if err != nil {
				return err
			}
			for {
				resp, err := stream.Recv()
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					return err
				}
				printRecord(cmd, resp.Record)
			}
			return nil
		},
	}
	cmd.Flags().Uint32Var(&limit, "limit", 0, "Max records to return (0 = all)")
	cmd.Flags().Uint32Var(&offset, "offset", 0, "Skip N records")
	cmd.Flags().StringVar(&orderBy, "order-by", "", "Field name to sort by")
	cmd.Flags().BoolVar(&descending, "descending", false, "Sort in descending order")
	return cmd
}

func findByIDCmd(flags *cliFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "get <collection> <id>",
		Short: "Get a record by id",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			_, client, cleanup, err := connect(flags)
			if err != nil {
				return err
			}
			defer cleanup()

			id, err := strconv.ParseUint(args[1], 10, 64)
			if err != nil {
				return fmt.Errorf("invalid id %q: %w", args[1], err)
			}
			resp, err := client.FindById(ctxWithAuth(flags), &pb.FindByIdRequest{Collection: args[0], Id: id})
			if err != nil {
				return err
			}
			printRecord(cmd, resp.Record)
			return nil
		},
	}
}

func updateCmd(flags *cliFlags) *cobra.Command {
	var txID string
	cmd := &cobra.Command{
		Use:   "update <collection> <id> <json>",
		Short: "Update a record by id",
		Args:  cobra.ExactArgs(3),
		RunE: func(cmd *cobra.Command, args []string) error {
			_, client, cleanup, err := connect(flags)
			if err != nil {
				return err
			}
			defer cleanup()

			id, err := strconv.ParseUint(args[1], 10, 64)
			if err != nil {
				return fmt.Errorf("invalid id: %w", err)
			}
			data, err := parseJSONArg(args[2])
			if err != nil {
				return err
			}
			ctx := ctxWithAuth(flags)
			if txID != "" {
				ctx = ctxWithTx(flags, txID)
			}
			resp, err := client.Update(ctx, &pb.UpdateRequest{
				Collection: args[0], Id: id, Data: data,
			})
			if err != nil {
				return err
			}
			if txID != "" {
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "staged update id:%d (tx:%s)\n", resp.Id, txID)
			} else {
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "updated id:%d (%s)\n", resp.Id, resp.DateModified)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&txID, "tx-id", "", "Stage this operation inside an open transaction")
	return cmd
}

func deleteCmd(flags *cliFlags) *cobra.Command {
	var txID string
	cmd := &cobra.Command{
		Use:   "delete <collection> <id>",
		Short: "Delete a record by id",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			_, client, cleanup, err := connect(flags)
			if err != nil {
				return err
			}
			defer cleanup()

			id, err := strconv.ParseUint(args[1], 10, 64)
			if err != nil {
				return fmt.Errorf("invalid id: %w", err)
			}
			ctx := ctxWithAuth(flags)
			if txID != "" {
				ctx = ctxWithTx(flags, txID)
			}
			_, err = client.Delete(ctx, &pb.DeleteRequest{
				Collection: args[0], Id: id,
			})
			if err != nil {
				return err
			}
			if txID != "" {
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "staged delete id:%d (tx:%s)\n", id, txID)
			} else {
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "deleted id:%d\n", id)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&txID, "tx-id", "", "Stage this operation inside an open transaction")
	return cmd
}

// ---- Stats ----------------------------------------------------------------

func statsCmd(flags *cliFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "stats <collection>",
		Short: "Show collection statistics",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			_, client, cleanup, err := connect(flags)
			if err != nil {
				return err
			}
			defer cleanup()

			resp, err := client.CollectionStats(ctxWithAuth(flags), &pb.CollectionStatsRequest{Collection: args[0]})
			if err != nil {
				return err
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(),
				"collection:%s  records:%d  segments:%d  dirty:%d  size:%d bytes\n",
				resp.Collection, resp.RecordCount, resp.SegmentCount, resp.DirtyEntries, resp.SizeBytes,
			)
			return nil
		},
	}
}

func compactCmd(flags *cliFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "compact <collection>",
		Short: "Force a synchronous compaction pass on a collection",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			_, client, cleanup, err := connect(flags)
			if err != nil {
				return err
			}
			defer cleanup()

			resp, err := client.Compact(ctxWithAuth(flags), &pb.CompactRequest{Collection: args[0]})
			if err != nil {
				return err
			}
			if resp.Ok {
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "compacted %s\n", args[0])
			}
			return nil
		},
	}
}

// ---- Export / Import ------------------------------------------------------

func exportCmd(flags *cliFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "export <collection>",
		Short: "Export all records as NDJSON to stdout",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			_, client, cleanup, err := connect(flags)
			if err != nil {
				return err
			}
			defer cleanup()

			stream, err := client.Find(ctxWithAuth(flags), &pb.FindRequest{Collection: args[0]})
			if err != nil {
				return err
			}
			enc := json.NewEncoder(cmd.OutOrStdout())
			for {
				resp, err := stream.Recv()
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					return err
				}
				_ = enc.Encode(resp.Record.Data.AsMap())
			}
			return nil
		},
	}
}

func importCmd(flags *cliFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "import <collection>",
		Short: "Import NDJSON records from stdin",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			_, client, cleanup, err := connect(flags)
			if err != nil {
				return err
			}
			defer cleanup()

			dec := json.NewDecoder(os.Stdin)
			var count int
			for dec.More() {
				var raw map[string]any
				if err := dec.Decode(&raw); err != nil {
					return fmt.Errorf("decode line %d: %w", count+1, err)
				}
				s, err := structpb.NewStruct(raw)
				if err != nil {
					return fmt.Errorf("struct line %d: %w", count+1, err)
				}
				if _, err := client.Insert(ctxWithAuth(flags), &pb.InsertRequest{
					Collection: args[0], Data: s,
				}); err != nil {
					return fmt.Errorf("insert line %d: %w", count+1, err)
				}
				count++
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "imported %d records\n", count)
			return nil
		},
	}
}

// ---- Secondary indexes ----------------------------------------------------

func ensureIndexCmd(flags *cliFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "ensure-index <collection> <field>",
		Short: "Create a secondary index on a field (idempotent)",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			_, client, cleanup, err := connect(flags)
			if err != nil {
				return err
			}
			defer cleanup()
			resp, err := client.EnsureIndex(ctxWithAuth(flags), &pb.EnsureIndexRequest{
				Collection: args[0], Field: args[1],
			})
			if err != nil {
				return err
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "index on %q ready for collection %q\n", resp.Field, resp.Collection)
			return nil
		},
	}
}

func dropIndexCmd(flags *cliFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "drop-index <collection> <field>",
		Short: "Drop the secondary index on a field",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			_, client, cleanup, err := connect(flags)
			if err != nil {
				return err
			}
			defer cleanup()
			if _, err := client.DropIndex(ctxWithAuth(flags), &pb.DropIndexRequest{
				Collection: args[0], Field: args[1],
			}); err != nil {
				return err
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "dropped index on %q\n", args[1])
			return nil
		},
	}
}

func listIndexesCmd(flags *cliFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "indexes <collection>",
		Short: "List all secondary indexes on a collection",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			_, client, cleanup, err := connect(flags)
			if err != nil {
				return err
			}
			defer cleanup()
			resp, err := client.ListIndexes(ctxWithAuth(flags), &pb.ListIndexesRequest{Collection: args[0]})
			if err != nil {
				return err
			}
			if len(resp.Fields) == 0 {
				_, _ = fmt.Fprintln(cmd.OutOrStdout(), "(no indexes)")
				return nil
			}
			for _, f := range resp.Fields {
				_, _ = fmt.Fprintln(cmd.OutOrStdout(), f)
			}
			return nil
		},
	}
}

// ---- Transactions ---------------------------------------------------------

func beginTxCmd(flags *cliFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "begin-tx <collection>",
		Short: "Begin a transaction and print its tx_id",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			_, client, cleanup, err := connect(flags)
			if err != nil {
				return err
			}
			defer cleanup()
			resp, err := client.BeginTx(ctxWithAuth(flags), &pb.BeginTxRequest{Collection: args[0]})
			if err != nil {
				return err
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "%s\n", resp.TxId)
			return nil
		},
	}
}

func commitTxCmd(flags *cliFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "commit-tx <tx_id>",
		Short: "Commit an open transaction",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			_, client, cleanup, err := connect(flags)
			if err != nil {
				return err
			}
			defer cleanup()
			if _, err := client.CommitTx(ctxWithAuth(flags), &pb.CommitTxRequest{TxId: args[0]}); err != nil {
				return err
			}
			_, _ = fmt.Fprintln(cmd.OutOrStdout(), "committed")
			return nil
		},
	}
}

func rollbackTxCmd(flags *cliFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "rollback-tx <tx_id>",
		Short: "Rollback (discard) an open transaction",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			_, client, cleanup, err := connect(flags)
			if err != nil {
				return err
			}
			defer cleanup()
			if _, err := client.RollbackTx(ctxWithAuth(flags), &pb.RollbackTxRequest{TxId: args[0]}); err != nil {
				return err
			}
			_, _ = fmt.Fprintln(cmd.OutOrStdout(), "rolled back")
			return nil
		},
	}
}

// ctxWithTx returns a context carrying both the API key and a tx_id header.
func ctxWithTx(flags *cliFlags, txID string) context.Context {
	pairs := []string{"x-tx-id", txID}
	if flags.apiKey != "" {
		pairs = append(pairs, "x-api-key", flags.apiKey)
	}
	return metadata.NewOutgoingContext(context.Background(), metadata.Pairs(pairs...))
}

// ---- Helpers --------------------------------------------------------------

func parseJSONArg(s string) (*structpb.Struct, error) {
	var m map[string]any
	if err := json.Unmarshal([]byte(s), &m); err != nil {
		return nil, fmt.Errorf("invalid JSON: %w", err)
	}
	return structpb.NewStruct(m)
}

// parseFilterArg converts a simple {"field":"x","op":"eq","value":"y"} JSON
// arg into a pb.Filter. Also accepts {"and":[...]} and {"or":[...]}.
func parseFilterArg(s string) (*pb.Filter, error) {
	var raw map[string]any
	if err := json.Unmarshal([]byte(s), &raw); err != nil {
		return nil, err
	}
	return buildFilter(raw)
}

func buildFilter(raw map[string]any) (*pb.Filter, error) {
	if andRaw, ok := raw["and"]; ok {
		items, _ := andRaw.([]any)
		var filters []*pb.Filter
		for _, item := range items {
			m, _ := item.(map[string]any)
			f, err := buildFilter(m)
			if err != nil {
				return nil, err
			}
			filters = append(filters, f)
		}
		return &pb.Filter{Kind: &pb.Filter_And{And: &pb.AndFilter{Filters: filters}}}, nil
	}
	if orRaw, ok := raw["or"]; ok {
		items, _ := orRaw.([]any)
		var filters []*pb.Filter
		for _, item := range items {
			m, _ := item.(map[string]any)
			f, err := buildFilter(m)
			if err != nil {
				return nil, err
			}
			filters = append(filters, f)
		}
		return &pb.Filter{Kind: &pb.Filter_Or{Or: &pb.OrFilter{Filters: filters}}}, nil
	}

	field, _ := raw["field"].(string)
	opStr, _ := raw["op"].(string)
	// Re-encode the value as JSON so filter.go can unmarshal it correctly
	// regardless of whether it came in as a string, number, or bool.
	valBytes, _ := json.Marshal(raw["value"])
	val := string(valBytes)

	opMap := map[string]pb.FilterOp{
		"eq": pb.FilterOp_EQ, "neq": pb.FilterOp_NEQ,
		"gt": pb.FilterOp_GT, "gte": pb.FilterOp_GTE,
		"lt": pb.FilterOp_LT, "lte": pb.FilterOp_LTE,
		"contains": pb.FilterOp_CONTAINS, "regex": pb.FilterOp_REGEX,
	}
	op, ok := opMap[opStr]
	if !ok {
		op = pb.FilterOp_EQ
	}
	return &pb.Filter{Kind: &pb.Filter_Field{Field: &pb.FieldFilter{
		Field: field, Op: op, Value: val,
	}}}, nil
}

func printRecord(cmd *cobra.Command, r *pb.Record) {
	if r == nil {
		return
	}
	b, _ := json.Marshal(r.Data.AsMap())
	_, _ = fmt.Fprintf(cmd.OutOrStdout(), "id:%-6d  %s\n", r.Id, string(b))
}
