package eth

import (
	"bytes"
	"context"
	"encoding/hex"
	"sync"
	"testing"
	"time"

	"github.com/OffchainLabs/prysm/v7/beacon-chain/core/signing"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p/encoder"
	"github.com/OffchainLabs/prysm/v7/config/params"
	"github.com/OffchainLabs/prysm/v7/consensus-types/primitives"
	"github.com/OffchainLabs/prysm/v7/crypto/bls"
	enginev1 "github.com/OffchainLabs/prysm/v7/proto/engine/v1"
	ethtypes "github.com/OffchainLabs/prysm/v7/proto/prysm/v1alpha1"
	"github.com/OffchainLabs/prysm/v7/time/slots"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pubsubpb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/probe-lab/hermes/host"
)

// blockTopic is shaped like a real gossip topic so the fork-digest check has
// something to match against.
const testForkDigest = "abcdef12"

const blockTopic = "/eth2/" + testForkDigest + "/beacon_block/ssz_snappy"

// recordingDataStream captures the trace events the validator emits so tests can
// assert that a withheld message is still observed.
type recordingDataStream struct {
	host.NoopDataStream
	mu     sync.Mutex
	events []*host.TraceEvent
}

func (r *recordingDataStream) PutRecord(_ context.Context, evt *host.TraceEvent) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, evt)
	return nil
}

type validatorFixture struct {
	ps       *PubSub
	stream   *recordingDataStream
	slot     primitives.Slot
	proposer primitives.ValidatorIndex
	key      bls.SecretKey
	domain   []byte
}

// newValidatorFixture wires a PubSub with a chain whose genesis is placed so
// that `slot` is the current slot, and a Prysm client that cannot be used.
func newValidatorFixture(t testing.TB, cfg ValidationConfig) *validatorFixture {
	t.Helper()

	const slot = primitives.Slot(1024)
	const proposer = primitives.ValidatorIndex(42)

	secondsPerSlot := time.Duration(params.BeaconConfig().SecondsPerSlot) * time.Second
	genesisTime := time.Now().Add(-time.Duration(slot) * secondsPerSlot)

	digest, err := hex.DecodeString(testForkDigest)
	require.NoError(t, err)

	statusHolder := &StatusHolder{}
	statusHolder.SetV2(&ethtypes.StatusV2{ForkDigest: digest})

	chain := &Chain{
		Fork: deneb,
		cfg: &ChainConfig{
			GenesisConfig: &GenesisConfig{
				GenesisTime:          genesisTime,
				GenesisValidatorRoot: make([]byte, 32),
			},
		},
		statusHolder:   statusHolder,
		metadataHolder: &MetadataHolder{},
	}

	epoch := slots.ToEpoch(slot)
	fork, err := params.Fork(epoch)
	require.NoError(t, err)
	domain, err := signing.Domain(fork, epoch, params.BeaconConfig().DomainBeaconProposer, chain.cfg.GenesisConfig.GenesisValidatorRoot)
	require.NoError(t, err)

	key, err := bls.RandKey()
	require.NoError(t, err)

	stream := &recordingDataStream{}

	ps := &PubSub{
		cfg: &PubSubConfig{
			Chain:          chain,
			Encoder:        encoder.SszNetworkEncoder{},
			SecondsPerSlot: secondsPerSlot,
			DataStream:     stream,
			Validation:     cfg,
		},
		withheldC: make(chan *host.TraceEvent, withheldQueueSize),
	}

	return &validatorFixture{
		ps:       ps,
		stream:   stream,
		slot:     slot,
		proposer: proposer,
		key:      key,
		domain:   domain,
	}
}

func (f *validatorFixture) withheldEvents() []*host.TraceEvent {
	var out []*host.TraceEvent
	for {
		select {
		case evt := <-f.ps.withheldC:
			out = append(out, evt)
		default:
			return out
		}
	}
}

// signedBlock builds a Deneb block for the given slot, signed by key.
func (f *validatorFixture) signedBlock(t testing.TB, key bls.SecretKey, slot primitives.Slot, proposer primitives.ValidatorIndex) []byte {
	return f.signedBlockWithGraffiti(t, key, slot, proposer, "")
}

// signedBlockWithGraffiti varies the block body so two blocks can share a slot
// and proposer while hashing differently, which is what equivocation looks like.
func (f *validatorFixture) signedBlockWithGraffiti(t testing.TB, key bls.SecretKey, slot primitives.Slot, proposer primitives.ValidatorIndex, graffiti string) []byte {
	t.Helper()

	block := newTestBlockDeneb()
	block.Block.Slot = slot
	block.Block.ProposerIndex = proposer
	if graffiti != "" {
		g := make([]byte, 32)
		copy(g, graffiti)
		block.Block.Body.Graffiti = g
	}

	root, err := block.Block.HashTreeRoot()
	require.NoError(t, err)

	signingRoot, err := signing.ComputeSigningRootForRoot(root, f.domain)
	require.NoError(t, err)

	block.Signature = key.Sign(signingRoot[:]).Marshal()

	var buf bytes.Buffer
	_, err = f.ps.cfg.Encoder.EncodeGossip(&buf, block)
	require.NoError(t, err)
	return buf.Bytes()
}

func (f *validatorFixture) message(data []byte) *pubsub.Message {
	topic := blockTopic
	return &pubsub.Message{
		Message: &pubsubpb.Message{
			Data:  data,
			Topic: &topic,
		},
		ID:           "test-msg",
		ReceivedFrom: peer.ID("test-peer"),
	}
}

func TestValidateBeaconBlock(t *testing.T) {
	tests := []struct {
		name       string
		slotFor    func(f *validatorFixture) primitives.Slot
		mutate     func([]byte) []byte
		want       pubsub.ValidationResult
		wantRecord bool
	}{
		{
			name: "valid block is accepted",
			want: pubsub.ValidationAccept,
		},
		{
			name:       "truncated payload is rejected",
			mutate:     func(b []byte) []byte { return b[:len(b)/2] },
			want:       pubsub.ValidationReject,
			wantRecord: true,
		},
		{
			name:       "corrupt payload is rejected",
			mutate:     func(b []byte) []byte { out := append([]byte(nil), b...); out[len(out)-1] ^= 0xFF; return out },
			want:       pubsub.ValidationReject,
			wantRecord: true,
		},
		{
			name:       "slot far outside the window is ignored",
			slotFor:    func(f *validatorFixture) primitives.Slot { return f.slot + 512 },
			want:       pubsub.ValidationIgnore,
			wantRecord: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newValidatorFixture(t, ValidationConfig{Mode: ValidationModeStructural})

			slot := f.slot
			if tt.slotFor != nil {
				slot = tt.slotFor(f)
			}

			data := f.signedBlock(t, f.key, slot, f.proposer)
			if tt.mutate != nil {
				data = tt.mutate(data)
			}

			got := f.ps.validateBeaconBlock(context.Background(), peer.ID("test-peer"), f.message(data))
			assert.Equal(t, tt.want, got)

			events := f.withheldEvents()
			if tt.wantRecord {
				require.Len(t, events, 1, "a withheld message must still be recorded")
				assert.Equal(t, eventTypeWithheldMessage, events[0].Type)
			} else {
				assert.Empty(t, events, "accepted messages are recorded by the handler, not the validator")
			}
		})
	}
}
func TestValidateBeaconBlockWithheldEventsReachTheStream(t *testing.T) {
	f := newValidatorFixture(t, ValidationConfig{Mode: ValidationModeStructural})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go f.ps.serveWithheldEvents(ctx)

	bad := f.signedBlock(t, f.key, f.slot, f.proposer)
	bad = bad[:len(bad)/2]
	require.Equal(t, pubsub.ValidationReject,
		f.ps.validateBeaconBlock(ctx, peer.ID("p"), f.message(bad)))

	require.Eventually(t, func() bool {
		f.stream.mu.Lock()
		defer f.stream.mu.Unlock()
		return len(f.stream.events) == 1
	}, 2*time.Second, 10*time.Millisecond, "the withheld event should reach the data stream")
}

// TestEmitWithheldMessageNeverBlocks is the guard on the no-I/O invariant for the
// telemetry path: a full queue must drop rather than park a validation worker.
func TestEmitWithheldMessageNeverBlocks(t *testing.T) {
	f := newValidatorFixture(t, ValidationConfig{Mode: ValidationModeStructural})

	// Nothing is draining, so fill the queue past capacity.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < withheldQueueSize*3; i++ {
			f.ps.emitWithheldMessage(context.Background(), f.message([]byte("x")), "reject", reasonDecode, nil)
		}
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("emitWithheldMessage blocked with a full queue")
	}
	assert.Len(t, f.withheldEvents(), withheldQueueSize)
}

func TestValidationConfigValidate(t *testing.T) {
	for _, mode := range []ValidationMode{ValidationModeOff, ValidationModeStructural} {
		assert.NoError(t, ValidationConfig{Mode: mode}.Validate())
	}
	assert.Error(t, ValidationConfig{Mode: "nonsense"}.Validate())
}

func BenchmarkValidateBeaconBlock(b *testing.B) {
	f := newValidatorFixture(b, ValidationConfig{Mode: ValidationModeStructural})
	data := f.signedBlock(b, f.key, f.slot, f.proposer)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.ps.validateBeaconBlock(context.Background(), peer.ID("p"), f.message(data))
	}
}

// newTestBlockDeneb builds a Deneb block whose variable-length fields are sized
// so fastssz can marshal it. Hand-rolled rather than pulled from
// prysm/testing/util, which drags extra modules into go.mod for a test helper.
func newTestBlockDeneb() *ethtypes.SignedBeaconBlockDeneb {
	return &ethtypes.SignedBeaconBlockDeneb{
		Signature: make([]byte, 96),
		Block: &ethtypes.BeaconBlockDeneb{
			ParentRoot: make([]byte, 32),
			StateRoot:  make([]byte, 32),
			Body: &ethtypes.BeaconBlockBodyDeneb{
				RandaoReveal: make([]byte, 96),
				Graffiti:     make([]byte, 32),
				Eth1Data: &ethtypes.Eth1Data{
					DepositRoot: make([]byte, 32),
					BlockHash:   make([]byte, 32),
				},
				SyncAggregate: &ethtypes.SyncAggregate{
					SyncCommitteeBits:      make([]byte, 64),
					SyncCommitteeSignature: make([]byte, 96),
				},
				ExecutionPayload: &enginev1.ExecutionPayloadDeneb{
					ParentHash:    make([]byte, 32),
					FeeRecipient:  make([]byte, 20),
					StateRoot:     make([]byte, 32),
					ReceiptsRoot:  make([]byte, 32),
					LogsBloom:     make([]byte, 256),
					PrevRandao:    make([]byte, 32),
					ExtraData:     make([]byte, 0),
					BaseFeePerGas: make([]byte, 32),
					BlockHash:     make([]byte, 32),
					Transactions:  make([][]byte, 0),
					Withdrawals:   make([]*enginev1.Withdrawal, 0),
				},
			},
		},
	}
}
