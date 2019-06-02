package scheduler

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis"
	"github.com/json-iterator/go"
	"go.uber.org/zap"

	redisConfig "github.com/go-framework/config/redis"
	zapConfig "github.com/go-framework/zap"

	"github.com/go-framework/errors"
)

const (
	TimestampSeparator = "#"
)

// Redis queue defined.
// use redis hash store Runner, key s Runner'id, sort set store Runner hash key, score is Runner action time.
// TODO: hash and sort set sync.
type RedisQueue struct {
	// private hash key.
	// save when Pop, remove when Push.
	privateHashKey string
	// redis hash key.
	// save when push, remove when Remove.
	hashKey string
	// redis sort set key.
	// save when Push, remove when Pop.
	setKey string
	// redis client.
	client *redis.Client
	// store Runner for clone new Runner.
	types sync.Map
	// zap logger.
	logger *zap.Logger
}

// Register Runner into queuer.
func (this *RedisQueue) Register(runner Runner) {
	this.types.Store(runner.GetType(), runner)
}

// make Runner type.
func (this *RedisQueue) makeRunnerType(runner Runner) string {
	return runner.GetType() + "@"
}

// get Runner type and data.
func (this *RedisQueue) getRunnerType(raw string) (string, string, error) {
	// raw data contain Runner's type and json string.
	splits := strings.SplitN(raw, "@", 2)
	if len(splits) != 2 {
		return "", "", errors.New("not 2")
	}

	return splits[0], splits[1], nil
}

// get Runner from raw string.
func (this *RedisQueue) getRunner(raw string) (Runner, error) {
	typ, r, err := this.getRunnerType(raw)
	if err != nil {
		return nil, err
	}

	// load Runner's type from store.
	any, ok := this.types.Load(typ)
	if !ok {
		return nil, errors.New("not register runner type: " + typ)
	}
	// should be Runner type.
	runner, ok := any.(Runner)
	if !ok {
		return nil, errors.New("not Runner")
	}

	// new Runner object.
	object := runner.New()

	// unmarshal json string to Runner.
	err = jsoniter.Unmarshal([]byte(r), object)
	if err != nil {
		return nil, err
	}

	return object, nil
}

func (this *RedisQueue) getRunnerFromHash(ctx context.Context, key string, count, offset uint64) (runners []Runner, err error) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		// scan hash store keys. return filed and value.
		results, cursor, err := this.client.HScan(key, offset, "", int64(count)).Result()
		if err != nil {
			return nil, err
		}

		// range results.
		for i := 0; i < len(results); i += 2 {
			// get Runner.
			runner, err := this.getRunner(results[i+1])
			if err != nil {
				return nil, err
			}

			runners = append(runners, runner)
		}

		if cursor == 0 {
			return runners, nil
		}

		offset = cursor
	}
}

// Return the private hash key elements to the queue which greater than interval second.
func (this *RedisQueue) ReturnElements(ctx context.Context, interval int64) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:

		}
		// scan the keys match queue key.
		match := this.hashKey + "*"
		keys, cursor, err := this.client.Scan(0, match, 100).Result()
		if err != nil {
			return err
		}

		// no key break.
		if len(keys) == 0 {
			break
		}

		// range keys to return the matched elements to queue key.
		for _, key := range keys {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:

			}
			// split key and get the timestamp
			// format is [KEY][TimestampSeparator][timestamp].
			splits := strings.Split(key, TimestampSeparator)
			timestamp, err := strconv.ParseInt(splits[len(splits)-1], 10, 64)
			if err != nil {
				continue
			}

			// just return the key greater than interval.
			if time.Now().Unix()-timestamp < interval && interval > 0 {
				continue
			}

			// get runners from private hash set.
			count := uint64(100)
			offset := uint64(0)
			runners, err := this.getRunnerFromHash(ctx, this.privateHashKey, count, offset)
			if err != nil {
				return err
			}
			// push into Queuer.
			err = this.MultiPush(runners...)
			if err != nil {
				return err
			}
		}

		// get the cursor is 0 break loop.
		if cursor == 0 {
			break
		}
	}

	return nil
}

// Initialize queuer with context.
func (this *RedisQueue) Init(ctx context.Context) error {

	// get zap config from context.
	if config, ok := ZapConfigFromContext(ctx); ok {
		// get zap option from context.
		if opts, ok := ZapOptionFromContext(ctx); ok {
			this.logger = config.NewZapLogger(opts...)
		} else {
			this.logger = config.NewZapLogger()
		}
	}

	// if logger is nil then new it.
	if this.logger == nil {
		this.logger = zapConfig.GetDebugConfig().NewZapLogger()
	}

	return this.ReturnElements(ctx, 0)
}

const (
	redisPushScript = `
	--[[ 
	Keys:
		1 - sort set key, save when Push, remove when Pop and Remove.
		2 - hash key, save when push, remove when Remove.
		3 - private hash key, save when Pop, remove when Push.
	ARGV
		1 - runner id
		2 - runner raw data
		3 - runner action time as score
	]]--

-- return.
local ret = {}

ret["ok"] = true

-- save to hash key.
local ok = redis.call("HSET", KEYS[2], ARGV[1], ARGV[2])
if(ok)
then
	ok = redis.call("ZADD", KEYS[1], ARGV[3], ARGV[1])
	if(ok)
	then
		redis.call("HDEL", KEYS[3], ARGV[1])
	else
		ret["err"] = "call ZADD failed" 
	end
else
	ret["err"] = "call HSET failed" 
end

return ret
`
)

// Push one Runner into the queuer.
// sort set key, save when Push, remove when Pop and Remove.
// hash key, save when push, remove when Remove.
// private hash key, save when Pop, remove when Push.
func (this *RedisQueue) OnePush(runner Runner) (err error) {

	if runner == nil {
		return errors.New("runner is nil")
	}

	// params.
	keys := []string{this.setKey, this.hashKey, this.privateHashKey}
	var args []interface{}

	// reusable?
	if !runner.Reusable() {
		return errors.New("runner is not reusable")
	}

	// marshal Runner.
	data, err := jsoniter.Marshal(runner)
	if err != nil {
		return err
	}

	// set Runner type in queue format.
	typ := this.makeRunnerType(runner)

	// compose raw data.
	var raw []byte
	raw = append(raw, typ...)
	raw = append(raw, data...)

	// append id.
	args = append(args, runner.GetId()) // ARGV[1]
	// append raw data.
	args = append(args, raw) // ARGV[2]
	// append score.
	args = append(args, runner.GetActionTime()) // ARGV[3]

	// load redis script.
	cmd := redis.NewScript(redisPushScript)

	// run script.
	return cmd.Run(this.client, keys, args...).Err()
}

// Push multiple Runner into the queuer.
func (this *RedisQueue) MultiPush(runners ...Runner) (err error) {

	var ids []string // ARGV[2]

	// new pipeline.
	pipeline := this.client.TxPipeline()

	for _, runner := range runners {
		// runner is reusable?
		if !runner.Reusable() {
			err = errors.Append(err, fmt.Errorf("%s is not reusable", runner.GetId()))
			continue
		}

		// marshal Runner.
		data, err := jsoniter.Marshal(runner)
		if err != nil {
			return err
		}

		// append ids.
		ids = append(ids, runner.GetId())

		// set Runner type in queue format.
		typ := this.makeRunnerType(runner)

		// compose raw data.
		var raw []byte

		raw = append(raw, typ...)
		raw = append(raw, data...)

		// redis sort member.
		member := redis.Z{Score: float64(runner.GetActionTime()), Member: runner.GetId()}

		// add to hash set.
		err = pipeline.HSet(this.hashKey, runner.GetId(), raw).Err()
		if err != nil {
			return nil
		}
		// add to sort set.
		err = pipeline.ZAdd(this.setKey, member).Err()
		if err != nil {
			return nil
		}
		// del private hash set.
		pipeline.HDel(this.privateHashKey, runner.GetId())
	}

	// pipeline exec.
	if _, e := pipeline.Exec(); e != nil {
		err = errors.Append(err, e)
	}

	return
}

// Push Runners into the Queuer.
func (this *RedisQueue) Push(runners ...Runner) (err error) {
	length := len(runners)

	if length == 0 {
		return errors.New("runners length is 0")
	}

	// so many then use one push.
	if length > 100 {
		for _, runner := range runners {
			e := this.OnePush(runner)
			if e != nil {
				err = errors.Append(err, fmt.Errorf("%s %v", runner.GetId(), e))
			}
		}
		return
	}

	return this.MultiPush(runners...)
}

const (
	redisPopScript = `
	--[[ 
	Keys:
		1 - sort set key, save when Push, remove when Pop and Remove.
		2 - hash key, save when push, remove when Remove.
		3 - private hash key, save when Pop, remove when Push.
	ARGV
		1 - timestamp
		2 - offset, script will remove member then never use it.
		3 - count
	]]--

-- store members.
local members = {}

-- get member which score less than ARGV[1]:timestamp.
-- result is a array not with score, format is [member,member,...].
local result = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", ARGV[1], "LIMIT", ARGV[2], ARGV[3])

-- range result
for i = 1, #result, 1 do
	-- get data from hash.
	local data = redis.call("HGET", KEYS[2], result[i])

	-- HGET succeed.
	if(data)
	then
		-- insert to return.
		table.insert(members, data)

		-- save to private key.
		local ok = redis.call("HSET", KEYS[3], result[i], data)
		
		-- save succeed.
		if(ok)
		then
			-- remove member.
			redis.call("ZREM", KEYS[1], result[i])
		end
	end
end

return members
`
)

// Pop runner.
func (this *RedisQueue) Pop(ctx context.Context, count, offset uint64) (runners []Runner, err error) {

	// load redis script.
	cmd := redis.NewScript(redisPopScript)

	// params.
	keys := []string{this.setKey, this.hashKey, this.privateHashKey}
	args := []interface{}{time.Now().Unix(), 0, count}

	// run script.
	data, err := cmd.Run(this.client, keys, args...).Result()

	if err != nil {
		return nil, err
	}

	// convert type.
	slice, ok := data.([]interface{})
	if !ok {
		return nil, errors.New("not []interface")
	}

	// range slice.
	for _, item := range slice {
		// data should be string type.
		raw, ok := item.(string)
		if !ok {
			return nil, errors.New("not a string")
		}

		// get Runner.
		runner, err := this.getRunner(raw)
		if err != nil {
			return nil, err
		}

		// append to runners.
		runners = append(runners, runner)
	}

	return
}

const redisRemoveScript = `
	--[[ 
	Keys:
		1 - sort set key, save when Push, remove when Pop and Remove.
		2 - hash key, save when push, remove when Remove.
	ARGV
		1 - runner id
	]]--

redis.call("ZREM", KEYS[1], ARGV[1])
redis.call("HDEL", KEYS[2], ARGV[1])

return 1
`

// Remove runner from the queuer.
func (this *RedisQueue) Remove(runner Runner) error {
	// load redis script.
	cmd := redis.NewScript(redisRemoveScript)
	// run script.
	return cmd.Run(this.client, []string{this.setKey, this.hashKey}, runner.GetId()).Err()
}

// List all Runner in Queuer.
func (this *RedisQueue) List(ctx context.Context, count, offset uint64) (runners []Runner, err error) {
	return this.getRunnerFromHash(ctx, this.hashKey, count, offset)
}

// Length of queue.
func (this *RedisQueue) Length() int64 {
	return this.client.ZCard(this.setKey).Val()
	// return this.client.HLen(this.hashKey).Val()
}

// Waiting length of Queuer.
func (this *RedisQueue) Waiting() int64 {
	return this.client.ZCount(this.setKey, "0", strconv.Itoa(int(time.Now().Unix()))).Val()
}

// Implement Marshaler.
func (this *RedisQueue) MarshalJSON() ([]byte, error) {

	offset := uint64(0)

	buf := bytes.Buffer{}

	buf.WriteByte('[')
	defer buf.WriteByte(']')

	for {
		// scan hash store keys. return filed and value.
		results, cursor, err := this.client.HScan(this.hashKey, offset, "", 30).Result()
		if err != nil {
			return nil, err
		}
		// range results.
		for i := 0; i < len(results); i += 2 {
			_, data, err := this.getRunnerType(results[i+1])
			if err != nil {
				continue
			}

			buf.WriteString(data)
			buf.WriteByte(',')
		}

		if cursor == 0 {
			return buf.Bytes(), nil
		}

		offset = cursor
	}
}

// New redis queue.
func NewRedisQueue(key string, config *redisConfig.Config) Queuer {

	queue := &RedisQueue{
		client:         config.NewGoClient(),
		privateHashKey: fmt.Sprintf("task@%s#%d", key, time.Now().Unix()),
		hashKey:        "task@" + key,
		setKey:         "queue@" + key,
	}

	return queue
}
