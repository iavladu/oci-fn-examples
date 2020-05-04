import io
import json
import logging

from fdk import response


def handler(ctx, data: io.BytesIO=None):
    name = "World"
    try:
        body = json.loads(data.getvalue())
        name = body.get("name")
    except (Exception, ValueError) as ex:
        print(str(ex))
    
    return response.Response(
        ctx, response_data=json.dumps(
            {"Message": "Hello {0}".format(name),
            "ctx.AppID" : ctx.AppID(),
            "ctx.FnID" : ctx.FnID(),
            "ctx.CallID" : ctx.CallID(),
            "ctx.Config" : dict(ctx.Config()),
            "ctx.Headers" : ctx.Headers(),
            "ctx.Deadline" : ctx.Deadline(),
            "ctx.RequestURL": ctx.RequestURL(),
            "ctx.Method": ctx.Method(),
            "ctx.Format" : ctx.Format()}),
        headers={"Content-Type": "application/json"}
    )
