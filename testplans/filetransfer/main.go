package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"github.com/dustin/go-humanize"
	ma "github.com/multiformats/go-multiaddr"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"path/filepath"
	goruntime "runtime"
	"strings"
	"time"

	allselector "github.com/hannahhoward/all-selector"
	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	dss "github.com/ipfs/go-datastore/sync"
	badgerds "github.com/ipfs/go-ds-badger"
	"github.com/ipfs/go-graphsync/storeutil"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	chunk "github.com/ipfs/go-ipfs-chunker"
	offline "github.com/ipfs/go-ipfs-exchange-offline"
	files "github.com/ipfs/go-ipfs-files"
	format "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-merkledag"
	"github.com/ipfs/go-unixfs/importer/balanced"
	ihelper "github.com/ipfs/go-unixfs/importer/helpers"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/libp2p/go-libp2p-core/metrics"
	"github.com/testground/sdk-go/network"
	"golang.org/x/sync/errgroup"

	gs "github.com/ipfs/go-graphsync"
	gsi "github.com/ipfs/go-graphsync/impl"
	gsnet "github.com/ipfs/go-graphsync/network"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	noise "github.com/libp2p/go-libp2p-noise"
	secio "github.com/libp2p/go-libp2p-secio"
	tls "github.com/libp2p/go-libp2p-tls"

	"github.com/testground/sdk-go/run"
	"github.com/testground/sdk-go/runtime"
	"github.com/testground/sdk-go/sync"
)

type AddrInfo struct {
	peerAddr *peer.AddrInfo
	ip       net.IP
}

func (pi AddrInfo) MarshalJSON() ([]byte, error) {
	out := make(map[string]interface{})
	peerJSON, err := pi.peerAddr.MarshalJSON()
	if err != nil {
		panic(fmt.Sprintf("error marshaling: %v", err))
	}
	out["PEER"] = string(peerJSON)

	ip, err := pi.ip.MarshalText()
	if err != nil {
		panic(fmt.Sprintf("error marshaling: %v", err))
	}
	out["IP"] = string(ip)
	return json.Marshal(out)
}

func (pi *AddrInfo) UnmarshalJSON(b []byte) error {
	var data map[string]interface{}
	err := json.Unmarshal(b, &data)
	if err != nil {
		panic(fmt.Sprintf("error unmarshaling: %v", err))
	}

	var pa peer.AddrInfo
	pi.peerAddr = &pa
	peerAddrData := data["PEER"].(string)
	var peerData map[string]interface{}
	err = json.Unmarshal([]byte(peerAddrData), &peerData)
	if err != nil {
		panic(err)
	}
	pid, err := peer.Decode(peerData["ID"].(string))
	if err != nil {
		panic(err)
	}
	pi.peerAddr.ID = pid
	addrs, ok := peerData["Addrs"].([]interface{})
	if ok {
		for _, a := range addrs {
			pi.peerAddr.Addrs = append(pi.peerAddr.Addrs, ma.StringCast(a.(string)))
		}
	}

	if err := pi.ip.UnmarshalText([]byte(data["IP"].(string))); err != nil {
		panic(fmt.Sprintf("error unmarshaling: %v", err))
	}
	return nil
}

var testcases = map[string]interface{}{
	"stress": run.InitializedTestCaseFn(runStress),
}

func main() {
	run.InvokeMap(testcases)
}

type networkParams struct {
	latency   time.Duration
	bandwidth uint64
	size      uint64
}

func (p networkParams) String() string {
	return fmt.Sprintf("<lat: %s, bandwidth: %d>", p.latency, p.bandwidth)
}

func runStress(runenv *runtime.RunEnv, initCtx *run.InitContext) error {
	var (
		concurrency = runenv.IntParam("concurrency")

		networkParams = parseNetworkConfig(runenv)
	)
	runenv.RecordMessage("started test instance")
	runenv.RecordMessage("network params: %v", networkParams)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Minute)
	defer cancel()

	initCtx.MustWaitAllInstancesInitialized(ctx)

	host, ip, peers, _ := makeHost(ctx, runenv, initCtx)
	defer host.Close()

	var (
		// make datastore, blockstore, dag service, graphsync
		bs     = blockstore.NewBlockstore(dss.MutexWrap(createDatastore(runenv.BooleanParam("disk_store"))))
		dagsrv = merkledag.NewDAGService(blockservice.New(bs, offline.Exchange(bs)))
		gsync  = gsi.New(ctx,
			gsnet.NewFromLibp2pHost(host),
			storeutil.LoaderForBlockstore(bs),
			storeutil.StorerForBlockstore(bs),
		)
	)

	defer initCtx.SyncClient.MustSignalAndWait(ctx, "done", runenv.TestInstanceCount)

	switch runenv.TestGroupID {
	case "providers":
		if runenv.TestGroupInstanceCount > 1 {
			panic("test case only supports one provider")
		}

		runenv.RecordMessage("we are the provider")
		defer runenv.RecordMessage("done provider")

		gsync.RegisterIncomingRequestHook(func(p peer.ID, request gs.RequestData, hookActions gs.IncomingRequestHookActions) {
			hookActions.ValidateRequest()
		})

		return runProvider(ctx, runenv, initCtx, dagsrv, ip, networkParams, concurrency)

	case "requestors":
		runenv.RecordMessage("we are the requestor")
		defer runenv.RecordMessage("done requestor")

		p := peers[0]
		if err := host.Connect(ctx, *p.peerAddr); err != nil {
			return err
		}
		runenv.RecordMessage("done dialling provider")
		return runRequestor(ctx, runenv, initCtx, gsync, p, dagsrv, networkParams, concurrency)

	default:
		panic(fmt.Sprintf("unsupported group ID: %s\n", runenv.TestGroupID))
	}
}

func parseNetworkConfig(runenv *runtime.RunEnv) []networkParams {
	var (
		bandwidths = runenv.SizeArrayParam("bandwidths")
		latencies  []time.Duration
		sizes      = runenv.SizeArrayParam("sizes")
	)

	lats := runenv.StringArrayParam("latencies")
	for _, l := range lats {
		d, err := time.ParseDuration(l)
		if err != nil {
			panic(err)
		}
		latencies = append(latencies, d)
	}

	// prepend bandwidth=0 and latency=0 zero values; the first iteration will
	// be a control iteration. The sidecar interprets zero values as no
	// limitation on that attribute.
	bandwidths = append([]uint64{0}, bandwidths...)
	latencies = append([]time.Duration{0}, latencies...)

	var ret []networkParams
	for _, bandwidth := range bandwidths {
		for _, latency := range latencies {
			for _, size := range sizes {
				ret = append(ret, networkParams{
					latency:   latency,
					bandwidth: bandwidth,
					size:      size,
				})
			}
		}
	}
	return ret
}

func runRequestor(ctx context.Context, runenv *runtime.RunEnv, initCtx *run.InitContext, gsync gs.GraphExchange, p *AddrInfo, dagsrv format.DAGService, networkParams []networkParams, concurrency int) error {
	var (
		cids []cid.Cid
		// create a selector for the whole UnixFS dag
		sel = allselector.AllSelector
	)

	for round, np := range networkParams {
		var (
			topicCid  = sync.NewTopic(fmt.Sprintf("cid-%d", round), []cid.Cid{})
			stateNext = sync.State(fmt.Sprintf("next-%d", round))
			stateNet  = sync.State(fmt.Sprintf("network-configured-%d", round))
		)

		// wait for all instances to be ready for the next state.
		initCtx.SyncClient.MustSignalAndWait(ctx, stateNext, runenv.TestInstanceCount)

		// clean up previous CIDs to attempt to free memory
		// TODO does this work?
		_ = dagsrv.RemoveMany(ctx, cids)

		runenv.RecordMessage("===== ROUND %d: latency=%s, bandwidth=%d, size=%d =====", round, np.latency, np.bandwidth, np.size)

		sctx, scancel := context.WithCancel(ctx)
		cidCh := make(chan []cid.Cid, 1)
		initCtx.SyncClient.MustSubscribe(sctx, topicCid, cidCh)
		cids = <-cidCh
		scancel()

		// run GC to get accurate-ish stats.
		goruntime.GC()
		goruntime.GC()

		<-initCtx.SyncClient.MustBarrier(ctx, stateNet, 1).C

		errgrp, grpctx := errgroup.WithContext(ctx)
		for _, c := range cids {
			c := c   // capture
			np := np // capture

			errgrp.Go(func() error {
				measurement := fmt.Sprintf("duration.sec,lat=%s,bw=%s,concurrency=%d,size=%s", np.latency, humanize.IBytes(np.bandwidth), concurrency, humanize.Bytes(np.size))
				measurement = strings.Replace(measurement, " ", "", -1)

				// make a go-ipld-prime link for the root UnixFS node
				clink := cidlink.Link{Cid: c}

				// execute the traversal.
				runenv.RecordMessage("\t>>> requesting CID %s", c)

				start := time.Now()
				_, errCh := gsync.Request(grpctx, p.peerAddr.ID, clink, sel)
				for err := range errCh {
					return err
				}
				dur := time.Since(start)

				runenv.RecordMessage("\t<<< graphsync request complete with no errors")
				runenv.RecordMessage("***** ROUND %d observed graphsync duration (lat=%s,bw=%d,s=%d): %s", round, np.latency, np.bandwidth, np.size, dur)
				runenv.R().RecordPoint(measurement+",transport=graphsync", float64(dur)/float64(time.Second))

				// verify that we have the CID now.
				if node, err := dagsrv.Get(grpctx, c); err != nil {
					return err
				} else if node == nil {
					return fmt.Errorf("finished graphsync request, but CID not in store")
				}

				// request file directly over http
				start = time.Now()
				file, err := ioutil.TempFile(os.TempDir(), fmt.Sprintf("%s-", c.String()))
				if err != nil {
					panic(err)
				}
				resp, err := http.Get(fmt.Sprintf("http://%s:8080/%s", p.ip.String(), c.String()))
				if err != nil {
					panic(err)
				}
				bytesRead, err := io.Copy(file, resp.Body)
				if err != nil {
					panic(err)
				}
				dur = time.Since(start)

				runenv.RecordMessage(fmt.Sprintf("\t<<< http request complete with no errors, read %d bytes", bytesRead))
				runenv.RecordMessage("***** ROUND %d observed http duration (lat=%s,bw=%d,s=%d): %s", round, np.latency, np.bandwidth, np.size, dur)
				runenv.R().RecordPoint(measurement+",transport=http", float64(dur)/float64(time.Second))

				return nil
			})
		}

		if err := errgrp.Wait(); err != nil {
			return err
		}
	}

	return nil
}

func runProvider(ctx context.Context, runenv *runtime.RunEnv, initCtx *run.InitContext, dagsrv format.DAGService, ip net.IP, networkParams []networkParams, concurrency int) error {
	var (
		cids       []cid.Cid
		bufferedDS = format.NewBufferedDAG(ctx, dagsrv)
	)

	// start an http server on port 8080
	runenv.RecordMessage("creating http server at http://%s:8080", ip.String())
	svr := &http.Server{Addr: ":8080"}

	go func() {
		if err := svr.ListenAndServe(); err != nil {
			runenv.RecordMessage("shutdown http server at http://%s:8080", ip.String())
		}
	}()

	for round, np := range networkParams {
		var (
			topicCid  = sync.NewTopic(fmt.Sprintf("cid-%d", round), []cid.Cid{})
			stateNext = sync.State(fmt.Sprintf("next-%d", round))
			stateNet  = sync.State(fmt.Sprintf("network-configured-%d", round))
		)

		// wait for all instances to be ready for the next state.
		initCtx.SyncClient.MustSignalAndWait(ctx, stateNext, runenv.TestInstanceCount)

		// remove the previous CIDs from the dag service; hopefully this
		// will delete them from the store and free up memory.
		for _, c := range cids {
			_ = dagsrv.Remove(ctx, c)
		}
		cids = cids[:0]

		runenv.RecordMessage("===== ROUND %d: latency=%s, bandwidth=%d =====", round, np.latency, np.bandwidth)

		// generate as many random files as the concurrency level.
		for i := 0; i < concurrency; i++ {
			// file with random data
			data := files.NewReaderFile(io.LimitReader(rand.Reader, int64(np.size)))
			file, err := ioutil.TempFile(os.TempDir(), "unixfs-")
			if err != nil {
				panic(err)
			}
			if _, err := io.Copy(file, data); err != nil {
				panic(err)
			}

			unixfsChunkSize := uint64(1) << runenv.IntParam("chunk_size")
			unixfsLinksPerLevel := runenv.IntParam("links_per_level")

			params := ihelper.DagBuilderParams{
				Maxlinks:   unixfsLinksPerLevel,
				RawLeaves:  runenv.BooleanParam("raw_leaves"),
				CidBuilder: nil,
				Dagserv:    bufferedDS,
			}

			if _, err := file.Seek(0, 0); err != nil {
				panic(err)
			}
			db, err := params.New(chunk.NewSizeSplitter(file, int64(unixfsChunkSize)))
			if err != nil {
				return fmt.Errorf("unable to setup dag builder: %w", err)
			}

			node, err := balanced.Layout(db)
			if err != nil {
				return fmt.Errorf("unable to create unix fs node: %w", err)
			}

			// set up http server to send file
			http.HandleFunc(fmt.Sprintf("/%s", node.Cid()), func(w http.ResponseWriter, r *http.Request) {
				fileReader, err := os.Open(file.Name())
				defer fileReader.Close()
				if err != nil {
					panic(err)
				}
				_, err = io.Copy(w, fileReader)
				if err != nil {
					panic(err)
				}
			})

			cids = append(cids, node.Cid())
		}

		if err := bufferedDS.Commit(); err != nil {
			return fmt.Errorf("unable to commit unix fs node: %w", err)
		}

		// run GC to get accurate-ish stats.
		goruntime.GC()
		goruntime.GC()

		runenv.RecordMessage("\tCIDs are: %v", cids)
		initCtx.SyncClient.MustPublish(ctx, topicCid, cids)

		runenv.RecordMessage("\tconfiguring network for round %d", round)
		initCtx.NetClient.MustConfigureNetwork(ctx, &network.Config{
			Network: "default",
			Enable:  true,
			Default: network.LinkShape{
				Latency:   np.latency,
				Bandwidth: np.bandwidth * 8, // bps
			},
			CallbackState:  stateNet,
			CallbackTarget: 1,
		})
		runenv.RecordMessage("\tnetwork configured for round %d", round)
	}

	if err := svr.Shutdown(ctx); err != nil {
		panic(err)
	}

	return nil
}

func makeHost(ctx context.Context, runenv *runtime.RunEnv, initCtx *run.InitContext) (host.Host, net.IP, []*AddrInfo, *metrics.BandwidthCounter) {
	secureChannel := runenv.StringParam("secure_channel")

	var security libp2p.Option
	switch secureChannel {
	case "noise":
		security = libp2p.Security(noise.ID, noise.New)
	case "secio":
		security = libp2p.Security(secio.ID, secio.New)
	case "tls":
		security = libp2p.Security(tls.ID, tls.New)
	}

	// ☎️  Let's construct the libp2p node.
	ip := initCtx.NetClient.MustGetDataNetworkIP()
	listenAddr := fmt.Sprintf("/ip4/%s/tcp/0", ip)
	bwcounter := metrics.NewBandwidthCounter()
	host, err := libp2p.New(ctx,
		security,
		libp2p.ListenAddrStrings(listenAddr),
		libp2p.BandwidthReporter(bwcounter),
	)
	if err != nil {
		panic(fmt.Sprintf("failed to instantiate libp2p instance: %s", err))
	}

	// Record our listen addrs.
	runenv.RecordMessage("my listen addrs: %v", host.Addrs())

	// Obtain our own address info, and use the sync service to publish it to a
	// 'peersTopic' topic, where others will read from.
	var (
		id = host.ID()
		ai = &peer.AddrInfo{ID: id, Addrs: host.Addrs()}

		// the peers topic where all instances will advertise their AddrInfo.
		peersTopic = sync.NewTopic("peers", new(AddrInfo))

		// initialize a slice to store the AddrInfos of all other peers in the run.
		peers = make([]*AddrInfo, 0, runenv.TestInstanceCount-1)
	)

	// Publish our own.
	initCtx.SyncClient.MustPublish(ctx, peersTopic, &AddrInfo{
		peerAddr: ai,
		ip:       ip,
	})

	// Now subscribe to the peers topic and consume all addresses, storing them
	// in the peers slice.
	peersCh := make(chan *AddrInfo)
	sctx, scancel := context.WithCancel(ctx)
	defer scancel()

	sub := initCtx.SyncClient.MustSubscribe(sctx, peersTopic, peersCh)

	// Receive the expected number of AddrInfos.
	for len(peers) < cap(peers) {
		select {
		case ai := <-peersCh:
			if ai.peerAddr.ID == id {
				continue // skip over ourselves.
			}
			peers = append(peers, ai)
		case err := <-sub.Done():
			panic(err)
		}
	}

	return host, ip, peers, bwcounter
}

func createDatastore(diskStore bool) ds.Datastore {
	if !diskStore {
		return ds.NewMapDatastore()
	}

	// create temporary directory for badger datastore
	path := filepath.Join(os.TempDir(), "datastore")
	if _, err := os.Stat(path); os.IsNotExist(err) {
		if err := os.MkdirAll(path, 0755); err != nil {
			panic(err)
		}
	} else if err != nil {
		panic(err)
	}

	// create disk based badger datastore
	defopts := badgerds.DefaultOptions
	defopts.SyncWrites = false
	defopts.Truncate = true
	datastore, err := badgerds.NewDatastore(path, &defopts)
	if err != nil {
		panic(err)
	}

	return datastore
}
