package redisrepo

import (
	// "container/list"
	"context"
	"errors"
	"fmt"

	"github.com/go-redis/redis/v8"
)

var redisclient *redis.Client

const REPO_KEY string = "GIT_REPO"

func Test() {
	println("Hello World!")
	redisclient.Get(context.Background(), "a")
}

func LsRefs(repoName string) ([]string, error) {
	var repoExistsCmd = redisclient.SIsMember(context.Background(), fmt.Sprintf("%s:repos", REPO_KEY), repoName)
	if repoExistsCmd.Err() != nil && repoExistsCmd.Err() != redis.Nil {
		return nil, repoExistsCmd.Err()
	} else if repoExistsCmd.Err() == redis.Nil || !repoExistsCmd.Val() {
		return nil, errors.New("repo does not exist")
	}

	var refnamesCmd = redisclient.Keys(context.Background(), fmt.Sprintf("%s:repo:%s:ref:*", REPO_KEY, repoName))
	if refnamesCmd.Err() != nil {
		return nil, refnamesCmd.Err()
	}
	var refNames = refnamesCmd.Val()
	var refObjs map[string]map[string]string
	refNames = refNames
	for _, refName := range refNames {
		var refCmd = redisclient.HGetAll(context.Background(), fmt.Sprintf("%s:repo:%s:ref:%s", REPO_KEY, repoName, refName))
		if refCmd.Err() != nil {
			return nil, refCmd.Err()
		}
		refObjs[refName] = refCmd.Val()
	}
	for _, refObj := range refObjs {
		if _, error := resolveObjId(refObjs, refObj); error != nil {
			return nil, error
		}
	}
}

func resolveObjId(refObjs map[string]map[string]string, refObj map[string]string) (string, error) {
	if refType, ok := refObj["type"]; ok {
		switch refType {
		case "commit":
			if objId, ok := refObj["obj-id"]; ok {
				return objId, nil
			} else {
				return "", errors.New("missing key 'obj-id' in reference of type 'commit'")
			}
		case "symref":
			if objId, ok := refObj["obj-id"]; ok {
				return objId, nil
			}
			if symrefTarget, ok := refObj["symref-target"]; ok {
				if symrefTargetObj, ok := refObjs[symrefTarget]; ok {
					return resolveObjId(refObjs, symrefTargetObj)
				} else {
					return "", errors.New(fmt.Sprintf("invalid 'symref-target': '%s'", symrefTarget))
				}
			} else {
				return "", errors.New("missing key 'symref-target' in reference of type 'symref'")
			}
		default:
			return "", errors.New(fmt.Sprintf("unsupported type '%s'", refType))
		}
	} else {
		return "", errors.New("missing key 'type'")
	}
}
