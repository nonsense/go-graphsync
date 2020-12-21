package messagequeue

import (
	"context"
	"fmt"
	allocator2 "github.com/ipfs/go-graphsync/responsemanager/allocator"
	"math/rand"
	"sync"
	"testing"
	"time"

	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/stretchr/testify/require"

	"github.com/ipfs/go-graphsync"
	gsmsg "github.com/ipfs/go-graphsync/message"
	gsnet "github.com/ipfs/go-graphsync/network"
	"github.com/ipfs/go-graphsync/notifications"
	"github.com/ipfs/go-graphsync/testutil"
)

type fakeMessageNetwork struct {
	connectError       error
	messageSenderError error
	messageSender      gsnet.MessageSender
	wait               *sync.WaitGroup
}

func (fmn *fakeMessageNetwork) ConnectTo(context.Context, peer.ID) error {
	return fmn.connectError
}

func (fmn *fakeMessageNetwork) NewMessageSender(context.Context, peer.ID) (gsnet.MessageSender, error) {
	fmn.wait.Done()
	if fmn.messageSenderError == nil {
		return fmn.messageSender, nil
	}
	return nil, fmn.messageSenderError
}

type fakeMessageSender struct {
	sendError    error
	fullClosed   chan<- struct{}
	reset        chan<- struct{}
	messagesSent chan<- gsmsg.GraphSyncMessage
}

func (fms *fakeMessageSender) SendMsg(ctx context.Context, msg gsmsg.GraphSyncMessage) error {
	fms.messagesSent <- msg
	return fms.sendError
}
func (fms *fakeMessageSender) Close() error { fms.fullClosed <- struct{}{}; return nil }
func (fms *fakeMessageSender) Reset() error { fms.reset <- struct{}{}; return nil }

func TestStartupAndShutdown(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()

	peer := testutil.GeneratePeers(1)[0]
	messagesSent := make(chan gsmsg.GraphSyncMessage)
	resetChan := make(chan struct{}, 1)
	fullClosedChan := make(chan struct{}, 1)
	messageSender := &fakeMessageSender{nil, fullClosedChan, resetChan, messagesSent}
	var waitGroup sync.WaitGroup
	messageNetwork := &fakeMessageNetwork{nil, nil, messageSender, &waitGroup}
	allocator := allocator2.NewAllocator(1<<30, 1<<30)

	messageQueue := New(ctx, peer, messageNetwork, allocator)
	messageQueue.Startup()
	id := graphsync.RequestID(rand.Int31())
	priority := graphsync.Priority(rand.Int31())
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	selector := ssb.Matcher().Node()
	root := testutil.GenerateCids(1)[0]

	waitGroup.Add(1)
	messageQueue.BuildMessage(0, func(b *gsmsg.Builder) {
		b.AddRequest(gsmsg.NewRequest(id, root, selector, priority))
	}, []notifications.Notifee{})

	testutil.AssertDoesReceive(ctx, t, messagesSent, "message was not sent")

	messageQueue.Shutdown()

	testutil.AssertDoesReceiveFirst(t, fullClosedChan, "message sender should be closed", resetChan, ctx.Done())
}

func TestShutdownDuringMessageSend(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()

	peer := testutil.GeneratePeers(1)[0]
	messagesSent := make(chan gsmsg.GraphSyncMessage)
	resetChan := make(chan struct{}, 1)
	fullClosedChan := make(chan struct{}, 1)
	messageSender := &fakeMessageSender{
		fmt.Errorf("Something went wrong"),
		fullClosedChan,
		resetChan,
		messagesSent}
	var waitGroup sync.WaitGroup
	messageNetwork := &fakeMessageNetwork{nil, nil, messageSender, &waitGroup}
	allocator := allocator2.NewAllocator(1<<30, 1<<30)

	messageQueue := New(ctx, peer, messageNetwork, allocator)
	messageQueue.Startup()
	id := graphsync.RequestID(rand.Int31())
	priority := graphsync.Priority(rand.Int31())
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	selector := ssb.Matcher().Node()
	root := testutil.GenerateCids(1)[0]

	// setup a message and advance as far as beginning to send it
	waitGroup.Add(1)
	messageQueue.BuildMessage(0, func(b *gsmsg.Builder) {
		b.AddRequest(gsmsg.NewRequest(id, root, selector, priority))
	}, []notifications.Notifee{})
	waitGroup.Wait()

	// now shut down
	messageQueue.Shutdown()

	// let the message send attempt complete and fail (as it would if
	// the connection were closed)
	testutil.AssertDoesReceive(ctx, t, messagesSent, "message send not attempted")

	// verify the connection is reset after a failed send attempt
	testutil.AssertDoesReceiveFirst(t, resetChan, "message sender was not reset", fullClosedChan, ctx.Done())

	// now verify after it's reset, no further retries, connection
	// resets, or attempts to close the connection, cause the queue
	// should realize it's shut down and stop processing
	// FIXME: this relies on time passing -- 100 ms to be exact
	// and we should instead mock out time as a dependency
	waitGroup.Add(1)
	testutil.AssertDoesReceiveFirst(t, ctx.Done(), "further message operations should not occur", messagesSent, resetChan, fullClosedChan)
}

func TestProcessingNotification(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()

	peer := testutil.GeneratePeers(1)[0]
	messagesSent := make(chan gsmsg.GraphSyncMessage)
	resetChan := make(chan struct{}, 1)
	fullClosedChan := make(chan struct{}, 1)
	messageSender := &fakeMessageSender{nil, fullClosedChan, resetChan, messagesSent}
	var waitGroup sync.WaitGroup
	messageNetwork := &fakeMessageNetwork{nil, nil, messageSender, &waitGroup}
	allocator := allocator2.NewAllocator(1<<30, 1<<30)

	messageQueue := New(ctx, peer, messageNetwork, allocator)
	messageQueue.Startup()
	waitGroup.Add(1)
	blks := testutil.GenerateBlocksOfSize(3, 128)

	responseID := graphsync.RequestID(rand.Int31())
	extensionName := graphsync.ExtensionName("graphsync/awesome")
	extension := graphsync.ExtensionData{
		Name: extensionName,
		Data: testutil.RandomBytes(100),
	}
	status := graphsync.RequestCompletedFull
	expectedTopic := "testTopic"
	notifee, verifier := testutil.NewTestNotifee(expectedTopic, 5)
	messageQueue.BuildMessage(0, func(b *gsmsg.Builder) {
		b.AddResponseCode(responseID, status)
		b.AddExtensionData(responseID, extension)
	}, []notifications.Notifee{notifee})

	// wait for send attempt
	waitGroup.Wait()

	var message gsmsg.GraphSyncMessage
	testutil.AssertReceive(ctx, t, messagesSent, &message, "message did not send")
	receivedBlocks := message.Blocks()
	for _, block := range receivedBlocks {
		testutil.AssertContainsBlock(t, blks, block)
	}
	firstResponse := message.Responses()[0]
	extensionData, found := firstResponse.Extension(extensionName)
	require.Equal(t, responseID, firstResponse.RequestID())
	require.Equal(t, status, firstResponse.Status())
	require.True(t, found)
	require.Equal(t, extension.Data, extensionData)

	verifier.ExpectEvents(ctx, t, []notifications.Event{
		Event{Name: Queued},
		Event{Name: Sent},
	})
	verifier.ExpectClose(ctx, t)
}

func TestDedupingMessages(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()

	peer := testutil.GeneratePeers(1)[0]
	messagesSent := make(chan gsmsg.GraphSyncMessage)
	resetChan := make(chan struct{}, 1)
	fullClosedChan := make(chan struct{}, 1)
	messageSender := &fakeMessageSender{nil, fullClosedChan, resetChan, messagesSent}
	var waitGroup sync.WaitGroup
	messageNetwork := &fakeMessageNetwork{nil, nil, messageSender, &waitGroup}
	allocator := allocator2.NewAllocator(1<<30, 1<<30)

	messageQueue := New(ctx, peer, messageNetwork, allocator)
	messageQueue.Startup()
	waitGroup.Add(1)
	id := graphsync.RequestID(rand.Int31())
	priority := graphsync.Priority(rand.Int31())
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	selector := ssb.Matcher().Node()
	root := testutil.GenerateCids(1)[0]

	messageQueue.BuildMessage(0, func(b *gsmsg.Builder) {
		b.AddRequest(gsmsg.NewRequest(id, root, selector, priority))
	}, []notifications.Notifee{})
	// wait for send attempt
	waitGroup.Wait()
	id2 := graphsync.RequestID(rand.Int31())
	priority2 := graphsync.Priority(rand.Int31())
	selector2 := ssb.ExploreAll(ssb.Matcher()).Node()
	root2 := testutil.GenerateCids(1)[0]
	id3 := graphsync.RequestID(rand.Int31())
	priority3 := graphsync.Priority(rand.Int31())
	selector3 := ssb.ExploreIndex(0, ssb.Matcher()).Node()
	root3 := testutil.GenerateCids(1)[0]

	messageQueue.BuildMessage(0, func(b *gsmsg.Builder) {
		b.AddRequest(gsmsg.NewRequest(id2, root2, selector2, priority2))
		b.AddRequest(gsmsg.NewRequest(id3, root3, selector3, priority3))
	}, []notifications.Notifee{})

	var message gsmsg.GraphSyncMessage
	testutil.AssertReceive(ctx, t, messagesSent, &message, "message did not send")

	requests := message.Requests()
	require.Len(t, requests, 1, "number of requests in first message was not 1")
	request := requests[0]
	require.Equal(t, id, request.ID())
	require.False(t, request.IsCancel())
	require.Equal(t, priority, request.Priority())
	require.Equal(t, selector, request.Selector())

	testutil.AssertReceive(ctx, t, messagesSent, &message, "message did not senf")

	requests = message.Requests()
	require.Len(t, requests, 2, "number of requests in second message was not 2")
	for _, request := range requests {
		if request.ID() == id2 {
			require.False(t, request.IsCancel())
			require.Equal(t, priority2, request.Priority())
			require.Equal(t, selector2, request.Selector())
		} else if request.ID() == id3 {
			require.False(t, request.IsCancel())
			require.Equal(t, priority3, request.Priority())
			require.Equal(t, selector3, request.Selector())
		} else {
			t.Fatal("incorrect request added to message")
		}
	}
}
