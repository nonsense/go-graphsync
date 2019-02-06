package message

import (
	"fmt"

	ggio "github.com/gogo/protobuf/io"
	pb "github.com/ipfs/go-graphsync/message/pb"
	gsselector "github.com/ipfs/go-graphsync/selector"
	ipld "github.com/ipfs/go-ipld-format"
)

// GraphSyncRequestID is a unique identifier for a GraphSync request.
type GraphSyncRequestID int32

// GraphSyncPriority a priority for a GraphSync request.
type GraphSyncPriority int32

// GraphSyncResponseStatusCode is a status returned for a GraphSync Request.
type GraphSyncResponseStatusCode int32

const (

	// GraphSync Response Status Codes

	// Informational Response Codes (partial)

	// RequestAcknowledged means the request was received and is being worked on.
	RequestAcknowledged = 10
	// AdditionalPeers means additional peers were found that may be able
	// to satisfy the request and contained in the extra block of the response.
	AdditionalPeers = 11
	// NotEnoughGas means fulfilling this request requires payment.
	NotEnoughGas = 12
	// OtherProtocol means a different type of response than GraphSync is
	// contained in extra.
	OtherProtocol = 13

	// Success Response Codes (request terminated)

	// RequestCompletedFull means the entire fulfillment of the GraphSync request
	// was sent back.
	RequestCompletedFull = 20
	// RequestCompletedPartial means the response is completed, and part of the
	// GraphSync request was sent back, but not the complete request.
	RequestCompletedPartial = 21

	// Error Response Codes (request terminated)

	// RequestRejected means the node did not accept the incoming request.
	RequestRejected = 30
	// RequestFailedBusy means the node is too busy, try again later. Backoff may
	// be contained in extra.
	RequestFailedBusy = 31
	// RequestFailedUnknown means the request failed for an unspecified reason. May
	// contain data about why in extra.
	RequestFailedUnknown = 32
	// RequestFailedLegal means the request failed for legal reasons.
	RequestFailedLegal = 33
	// RequestFailedContentNotFound means the respondent does not have the content.
	RequestFailedContentNotFound = 34
)

// GraphSyncRequest is an interface for accessing data on request contained in a
// GraphSyncMessage.
type GraphSyncRequest interface {
	Selector() gsselector.Selector
	Root() ipld.Node
	Priority() GraphSyncPriority
	ID() GraphSyncRequestID
	IsCancel() bool
}

// GraphSyncResponse is an interface for accessing data on a response sent back
// in a GraphSyncMessage.
type GraphSyncResponse interface {
	RequestID() GraphSyncRequestID
	Status() GraphSyncResponseStatusCode
	Response() gsselector.SelectionResponse
}

// GraphSyncMessage is interface that can be serialized and deserialized to send
// over the GraphSync network
type GraphSyncMessage interface {
	Requests() []GraphSyncRequest

	Responses() []GraphSyncResponse

	AddRequest(id GraphSyncRequestID,
		selector gsselector.Selector,
		root ipld.Node,
		priority GraphSyncPriority)

	Cancel(id GraphSyncRequestID)

	AddResponse(
		requestID GraphSyncRequestID,
		status GraphSyncResponseStatusCode,
		response gsselector.SelectionResponse)

	Exportable

	Loggable() map[string]interface{}
}

// Exportable is an interface that can serialize to a protobuf
type Exportable interface {
	ToProto() *pb.Message
}

type graphSyncRequest struct {
	selector gsselector.Selector
	root     ipld.Node
	priority GraphSyncPriority
	id       GraphSyncRequestID
	isCancel bool
}

type graphSyncResponse struct {
	requestID GraphSyncRequestID
	status    GraphSyncResponseStatusCode
	response  gsselector.SelectionResponse
}

type graphSyncMessage struct {
	requests  map[GraphSyncRequestID]*graphSyncRequest
	responses map[GraphSyncRequestID]*graphSyncResponse
}

// DecodeSelectorFunc is a function that can build a type that satisfies
// the Selector interface from a raw byte array.
type DecodeSelectorFunc func([]byte) gsselector.Selector

// DecodeRootNodeFunc is a function that can build a type that satisfies
// the ipld.Node interface from a raw byte array.
type DecodeRootNodeFunc func([]byte) ipld.Node

// DecodeSelectionResponseFunc is a function that can build a type that satisfies
// the SelectionResponse interface from a raw byte array.
type DecodeSelectionResponseFunc func([]byte) gsselector.SelectionResponse

// New initializes a new blank GraphSyncMessage
func New() GraphSyncMessage {
	return newMsg()
}

func newMsg() *graphSyncMessage {
	return &graphSyncMessage{
		requests:  make(map[GraphSyncRequestID]*graphSyncRequest),
		responses: make(map[GraphSyncRequestID]*graphSyncResponse),
	}
}

func newMessageFromProto(pbm pb.Message,
	decodeRootNode DecodeRootNodeFunc,
	decodeSelector DecodeSelectorFunc,
	decodeSelectionResponse DecodeSelectionResponseFunc) (GraphSyncMessage, error) {
	gsm := newMsg()
	for _, req := range pbm.Reqlist {
		selector := decodeSelector(req.Selector)
		root := decodeRootNode(req.Root)
		gsm.addRequest(GraphSyncRequestID(req.Id), selector, root, GraphSyncPriority(req.Priority), req.Cancel)
	}

	for _, res := range pbm.Reslist {
		selectionResponse := decodeSelectionResponse(res.Data)
		gsm.AddResponse(GraphSyncRequestID(res.Id), GraphSyncResponseStatusCode(res.Status), selectionResponse)
	}

	return gsm, nil
}

func (gsm *graphSyncMessage) Requests() []GraphSyncRequest {
	requests := make([]GraphSyncRequest, 0, len(gsm.requests))
	for _, request := range gsm.requests {
		requests = append(requests, request)
	}
	return requests
}

func (gsm *graphSyncMessage) Responses() []GraphSyncResponse {
	responses := make([]GraphSyncResponse, 0, len(gsm.responses))
	for _, response := range gsm.responses {
		responses = append(responses, response)
	}
	return responses
}

func (gsm *graphSyncMessage) Cancel(id GraphSyncRequestID) {
	delete(gsm.requests, id)
	gsm.addRequest(id, nil, nil, 0, true)
}

func (gsm *graphSyncMessage) AddRequest(id GraphSyncRequestID,
	selector gsselector.Selector,
	root ipld.Node,
	priority GraphSyncPriority,
) {
	gsm.addRequest(id, selector, root, priority, false)
}

func (gsm *graphSyncMessage) addRequest(id GraphSyncRequestID,
	selector gsselector.Selector,
	root ipld.Node,
	priority GraphSyncPriority,
	isCancel bool) {
	gsm.requests[id] = &graphSyncRequest{
		id:       id,
		selector: selector,
		root:     root,
		priority: priority,
		isCancel: isCancel,
	}
}

func (gsm *graphSyncMessage) AddResponse(requestID GraphSyncRequestID,
	status GraphSyncResponseStatusCode,
	response gsselector.SelectionResponse) {
	gsm.responses[requestID] = &graphSyncResponse{
		requestID: requestID,
		status:    status,
		response:  response,
	}
}

// FromPBReader can deserialize a protobuf message into a GraphySyncMessage.
func FromPBReader(pbr ggio.Reader,
	decodeRootNode DecodeRootNodeFunc,
	decodeSelector DecodeSelectorFunc,
	decodeSelectionResponse DecodeSelectionResponseFunc) (GraphSyncMessage, error) {
	pb := new(pb.Message)
	if err := pbr.ReadMsg(pb); err != nil {
		return nil, err
	}

	return newMessageFromProto(*pb, decodeRootNode, decodeSelector, decodeSelectionResponse)
}

func (gsm *graphSyncMessage) ToProto() *pb.Message {
	pbm := new(pb.Message)
	pbm.Reqlist = make([]pb.Message_Request, 0, len(gsm.requests))
	for _, request := range gsm.requests {
		pbm.Reqlist = append(pbm.Reqlist, pb.Message_Request{
			Id:       int32(request.id),
			Root:     request.root.RawData(),
			Selector: request.selector.RawData(),
			Priority: int32(request.priority),
			Cancel:   request.isCancel,
		})
	}

	pbm.Reslist = make([]pb.Message_Response, 0, len(gsm.responses))
	for _, response := range gsm.responses {
		pbm.Reslist = append(pbm.Reslist, pb.Message_Response{
			Id:     int32(response.requestID),
			Status: int32(response.status),
			Data:   response.response.RawData(),
		})
	}
	return pbm
}

func (gsm *graphSyncMessage) Loggable() map[string]interface{} {
	requests := make([]string, 0, len(gsm.requests))
	for _, request := range gsm.requests {
		requests = append(requests, fmt.Sprintf("%d", request.id))
	}
	responses := make([]string, 0, len(gsm.responses))
	for _, response := range gsm.responses {
		responses = append(responses, fmt.Sprintf("%d", response.requestID))
	}
	return map[string]interface{}{
		"requests":  requests,
		"responses": responses,
	}
}

func (gsr *graphSyncRequest) ID() GraphSyncRequestID        { return gsr.id }
func (gsr *graphSyncRequest) Root() ipld.Node               { return gsr.root }
func (gsr *graphSyncRequest) Selector() gsselector.Selector { return gsr.selector }
func (gsr *graphSyncRequest) Priority() GraphSyncPriority   { return gsr.priority }
func (gsr *graphSyncRequest) IsCancel() bool                { return gsr.isCancel }

func (gsr *graphSyncResponse) RequestID() GraphSyncRequestID          { return gsr.requestID }
func (gsr *graphSyncResponse) Status() GraphSyncResponseStatusCode    { return gsr.status }
func (gsr *graphSyncResponse) Response() gsselector.SelectionResponse { return gsr.response }
