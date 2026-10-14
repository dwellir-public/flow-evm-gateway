package requester

import (
	"cmp"
	"context"
	"fmt"
	"slices"

	"github.com/hashicorp/go-multierror"
	"github.com/onflow/cadence"
	"github.com/onflow/flow-go-sdk"
	"github.com/onflow/flow-go-sdk/access"
	"github.com/onflow/flow-go-sdk/access/grpc"
	flowGo "github.com/onflow/flow-go/model/flow"
	"github.com/rs/zerolog"
	"go.uber.org/ratelimit"

	errs "github.com/onflow/flow-evm-gateway/models/errors"
)

type sporkClient struct {
	firstHeight                    uint64
	lastHeight                     uint64
	client                         access.Client
	getEventsForHeightRangeLimiter ratelimit.Limiter
}

// contains checks if the provided height is withing the range of available heights
func (s *sporkClient) contains(height uint64) bool {
	return height >= s.firstHeight && height <= s.lastHeight
}

func (s *sporkClient) GetEventsForHeightRange(
	ctx context.Context, eventType string, startHeight uint64, endHeight uint64,
) ([]flow.BlockEvents, error) {
	s.getEventsForHeightRangeLimiter.Take()

	return s.client.GetEventsForHeightRange(ctx, eventType, startHeight, endHeight)
}

func (s *sporkClient) Close() error {
	return s.client.Close()
}

type sporkClients []*sporkClient

// addSpork will add a new spork host defined by the first and last height boundary in that spork.
func (s *sporkClients) add(logger zerolog.Logger, client access.Client) error {
	header, err := client.GetLatestBlockHeader(context.Background(), true)
	if err != nil {
		return fmt.Errorf("could not get latest height using the spork client: %w", err)
	}

	info, err := client.GetNodeVersionInfo(context.Background())
	if err != nil {
		return fmt.Errorf("could not get node info using the spork client: %w", err)
	}

	return s.addKnownRange(logger, client, info.NodeRootBlockHeight, header.Height)
}

func (s *sporkClients) addKnownRange(logger zerolog.Logger, client access.Client, first, last uint64) error {
	if first == 0 || first > last {
		return fmt.Errorf("invalid previous spork height range (%d, %d)", first, last)
	}

	logger.Info().
		Uint64("firstHeight", first).
		Uint64("lastHeight", last).
		Msg("adding spork client")

	*s = append(*s, &sporkClient{
		// The configured or probed node root is the first height available on this node.
		firstHeight: first,
		lastHeight:  last,
		client:      client,
		// TODO (JanezP): Make this configurable
		getEventsForHeightRangeLimiter: ratelimit.New(100, ratelimit.WithoutSlack),
	})

	// make sure clients are always sorted
	slices.SortFunc(*s, func(a, b *sporkClient) int {
		return cmp.Compare(a.firstHeight, b.firstHeight)
	})

	return nil
}

// get spork client that contains the height or nil if not found.
func (s *sporkClients) get(height uint64) access.Client {
	for _, spork := range *s {
		if spork.contains(height) {
			return spork.client
		}
	}

	return nil
}

// continuous checks if all the past spork clients create a continuous
// range of heights.
func (s *sporkClients) continuous() bool {
	for i := 0; i < len(*s)-1; i++ {
		if (*s)[i].lastHeight+1 != (*s)[i+1].firstHeight {
			return false
		}
	}

	return true
}

// CrossSporkClient is a wrapper around the Flow AN client that can
// access different AN APIs based on the height boundaries of the sporks.
//
// Each spork is defined with the last height included in that spork,
// based on the list we know which AN client to use when requesting the data.
//
// Any API that supports cross-spork access must have a defined function
// that shadows the original access Client function.
type CrossSporkClient struct {
	logger                 zerolog.Logger
	sporkClients           sporkClients
	currentSporkRootHeight uint64
	access.Client
}

// NewCrossSporkClient creates a new instance of the multi-spork client. When
// pastSporkRoots are provided, they define each historical client's first height
// in client order and avoid probing historical clients at startup.
func NewCrossSporkClient(
	currentSpork access.Client,
	pastSporks []access.Client,
	logger zerolog.Logger,
	chainID flowGo.ChainID,
	pastSporkRoots ...uint64,
) (*CrossSporkClient, error) {
	sporkRootBlockHeight := uint64(0)
	nodeRootBlockHeight := uint64(0)

	// Temp fix due to the fact that Emulator does not support the
	// GetNodeVersionInfo method.
	if chainID != flowGo.Emulator {
		info, err := currentSpork.GetNodeVersionInfo(context.Background())
		if err != nil {
			return nil, fmt.Errorf("failed to get node version info: %w", err)
		}

		if info.SporkRootBlockHeight != info.NodeRootBlockHeight {
			logger.Warn().
				Uint64("spork-root-block-height", info.SporkRootBlockHeight).
				Uint64("node-root-block-height", info.NodeRootBlockHeight).
				Msg("spork root block height is not equal to node root block height. syncing may fail due to missing blocks.")
		}

		// It's possible that the node's SporkRootBlockHeight != NodeRootBlockHeight if the node was
		// bootstrapped with a block after the spork root block. In this case, using SporkRootBlockHeight
		// for the checks in IsPastSpork and IsSporkRootBlockHeight is correct. However, there may be
		// missing blocks in the gap between the roots.
		sporkRootBlockHeight = info.SporkRootBlockHeight
		nodeRootBlockHeight = info.NodeRootBlockHeight
	}

	clients := sporkClients{}
	if len(pastSporkRoots) > 0 && len(pastSporkRoots) != len(pastSporks) {
		return nil, fmt.Errorf("number of previous spork root heights (%d) does not match number of clients (%d)", len(pastSporkRoots), len(pastSporks))
	}
	for i, c := range pastSporks {
		var err error
		if len(pastSporkRoots) == 0 {
			err = clients.add(logger, c)
		} else {
			last := nodeRootBlockHeight
			if i+1 < len(pastSporkRoots) {
				last = pastSporkRoots[i+1]
			}
			if last == 0 || pastSporkRoots[i] >= last {
				return nil, fmt.Errorf("previous spork root heights must be strictly increasing and below the current node root height")
			}
			err = clients.addKnownRange(logger, c, pastSporkRoots[i], last-1)
		}
		if err != nil {
			return nil, err
		}
	}

	if !clients.continuous() {
		return nil, fmt.Errorf("provided past-spork clients don't create a continuous range of heights")
	}

	// at this point, we've verified that the past spork clients form a continuous range of heights.
	// next, make sure the last spork client forms a continuous range with the current spork
	// client's node root block height. Note: this must be the NodeRootBlockHeight, not the
	// SporkRootBlockHeight, since this is the first height available on the node.
	if len(clients) > 0 && clients[len(clients)-1].lastHeight+1 != nodeRootBlockHeight {
		return nil, fmt.Errorf(
			"provided past-spork clients don't end at the node root block height (%d, %d)",
			clients[len(clients)-1].lastHeight,
			nodeRootBlockHeight,
		)
	}

	return &CrossSporkClient{
		logger:                 logger,
		currentSporkRootHeight: sporkRootBlockHeight,
		sporkClients:           clients,
		Client:                 currentSpork,
	}, nil
}

// IsPastSpork will check if the provided height is contained in the previous sporks.
func (c *CrossSporkClient) IsPastSpork(height uint64) bool {
	return height < c.currentSporkRootHeight
}

// IsSporkRootBlockHeight will check if the provided height is the spork root block height.
func (c *CrossSporkClient) IsSporkRootBlockHeight(height uint64) bool {
	return height == c.currentSporkRootHeight
}

// CurrentSporkRootHeight returns the spork root block height of the current spork.
func (c *CrossSporkClient) CurrentSporkRootHeight() uint64 {
	return c.currentSporkRootHeight
}

// getClientForHeight returns the client for the given height that contains the height range.
//
// If the height is not contained in any of the past spork clients we return an error.
// If the height is contained in the current spork client we return the current spork client,
// but that doesn't guarantee the height will be found, since the height might be bigger than the
// latest height in the current spork, which is not checked due to performance reasons.
func (c *CrossSporkClient) getClientForHeight(height uint64) (access.Client, error) {
	if !c.IsPastSpork(height) {
		return c.Client, nil
	}

	client := c.sporkClients.get(height)
	if client == nil {
		return nil, errs.NewHeightOutOfRangeError(height)
	}

	c.logger.Debug().
		Uint64("requested-cadence-height", height).
		Msg("using previous spork client")

	return client, nil
}

// GetLatestHeightForSpork returns the known end height for a previous spork,
// or queries the current spork's latest height.
func (c *CrossSporkClient) GetLatestHeightForSpork(ctx context.Context, height uint64) (uint64, error) {
	if c.IsPastSpork(height) {
		for _, spork := range c.sporkClients {
			if spork.contains(height) {
				return spork.lastHeight, nil
			}
		}
		return 0, errs.NewHeightOutOfRangeError(height)
	}

	client, err := c.getClientForHeight(height)
	if err != nil {
		return 0, err
	}

	block, err := client.GetLatestBlockHeader(ctx, true)
	if err != nil {
		return 0, err
	}
	return block.Height, nil
}

func (c *CrossSporkClient) GetBlockHeaderByHeight(
	ctx context.Context,
	height uint64,
) (*flow.BlockHeader, error) {
	client, err := c.getClientForHeight(height)
	if err != nil {
		return nil, err
	}
	return client.GetBlockHeaderByHeight(ctx, height)
}

func (c *CrossSporkClient) ExecuteScriptAtBlockHeight(
	ctx context.Context,
	height uint64,
	script []byte,
	arguments []cadence.Value,
) (cadence.Value, error) {
	client, err := c.getClientForHeight(height)
	if err != nil {
		return nil, err
	}
	return client.ExecuteScriptAtBlockHeight(ctx, height, script, arguments)
}

func (c *CrossSporkClient) SubscribeEventsByBlockHeight(
	ctx context.Context,
	startHeight uint64,
	filter flow.EventFilter,
	opts ...access.SubscribeOption,
) (<-chan flow.BlockEvents, <-chan error, error) {
	client, err := c.getClientForHeight(startHeight)
	if err != nil {
		return nil, nil, err
	}
	return client.SubscribeEventsByBlockHeight(ctx, startHeight, filter, opts...)
}

func (c *CrossSporkClient) GetEventsForHeightRange(
	ctx context.Context, eventType string, startHeight uint64, endHeight uint64,
) ([]flow.BlockEvents, error) {
	client, err := c.getClientForHeight(startHeight)
	if err != nil {
		return nil, err
	}
	endClient, err := c.getClientForHeight(endHeight)
	if err != nil {
		return nil, err
	}
	// there is one client reference per spork, so we can compare the clients
	if endClient != client {
		return nil, fmt.Errorf("invalid height range, end height %d is not in the same spork as start height %d", endHeight, startHeight)
	}
	return client.GetEventsForHeightRange(ctx, eventType, startHeight, endHeight)
}

func (c *CrossSporkClient) GetEventsForBlockHeader(
	ctx context.Context,
	eventType string,
	blockHeader *flow.BlockHeader,
) ([]flow.BlockEvents, error) {
	client, err := c.getClientForHeight(blockHeader.Height)
	if err != nil {
		return nil, err
	}
	return client.GetEventsForBlockIDs(ctx, eventType, []flow.Identifier{blockHeader.ID})
}

func (c *CrossSporkClient) SubscribeBlockHeadersFromStartHeight(
	ctx context.Context,
	startHeight uint64,
	blockStatus flow.BlockStatus,
) (<-chan *flow.BlockHeader, <-chan error, error) {
	client, err := c.getClientForHeight(startHeight)
	if err != nil {
		return nil, nil, err
	}
	grpcClient, ok := (client).(*grpc.Client)
	if !ok {
		return nil, nil, fmt.Errorf("unable to convert to Flow grpc.Client")
	}

	return grpcClient.SubscribeBlockHeadersFromStartHeight(ctx, startHeight, blockStatus)
}

func (c *CrossSporkClient) Close() error {
	var merr *multierror.Error

	for _, client := range c.sporkClients {
		if err := client.Close(); err != nil {
			merr = multierror.Append(merr, err)
		}
	}
	err := c.Client.Close()
	if err != nil {
		merr = multierror.Append(merr, err)
	}

	return merr.ErrorOrNil()
}
