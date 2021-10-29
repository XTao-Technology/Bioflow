package message

const XSTONE_API_RET_OK string = "success"
const XSTONE_API_RET_FAIL string = "failed"

type XstoneAPIResult struct {
    Status string
    Msg string
}