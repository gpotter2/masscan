#include "output.h"

// Enabled in output.h
#ifdef KAFKA

#include "masscan.h"
#include "masscan-status.h"
#include "pixie-sockets.h"
#include "util-logger.h"
#include <ctype.h>

/****************************************************************************
 ****************************************************************************/
static void
kafka_out_close(struct Output *out, FILE *fp)
{
    UNUSEDPARM(fp);
    UNUSEDPARM(out);
}

/****************************************************************************
 ****************************************************************************/
static void
kafka_out_open(struct Output *out, FILE *fp)
{
    UNUSEDPARM(fp);
    UNUSEDPARM(out);
}

static void
kafka_produce(struct Output *out, char* topic, char* buf, int buflen)
{
    rd_kafka_resp_err_t err;

retry:
    // Produce the payload
    err = rd_kafka_producev(
        out->kafka.rk, RD_KAFKA_V_TOPIC(topic),
        RD_KAFKA_V_VALUE(buf, buflen),
        /* Copy the message payload so the `buf` can
         * be reused for the next message. */
        RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY), RD_KAFKA_V_END);

    if (err) {
        LOG(0, "Failed to produce to topic %s: %s\n", topic,
                rd_kafka_err2str(err));

        if (err == RD_KAFKA_RESP_ERR__QUEUE_FULL) {
            rd_kafka_poll(out->kafka.rk,
                          1000 /*block for max 1000ms*/);
            goto retry;
        }
        return;
    }

    rd_kafka_poll(out->kafka.rk, 0 /*non-blocking*/);
}

/****************************************************************************
 ****************************************************************************/
static void
kafka_out_status(struct Output *out, FILE *fp, time_t timestamp,
    int status, ipaddress ip, unsigned ip_proto, unsigned port, unsigned reason, unsigned ttl)
{
    UNUSEDPARM(fp);

    char buf[1024]; // The json data
    int buflen;
    
    char topic[32];
    char reason_buffer[128];

    ipaddress_formatted_t fmt = ipaddress_fmt(ip);
   
    // Build the kafka topic name 
    snprintf(topic, sizeof(topic),
        "%s-%u",
        name_from_ip_proto(ip_proto),
        port
    );

    // Build the json payload
    buflen = snprintf(buf, sizeof(buf),
        "{"
        "\"ip\":\"%s\",\"timestamp\": \"%d\","
        "\"ports\":[{\"port\":%u,\"proto\":\"%s\",\"status\":\"%s\","
        "\"reason\":\"%s\",\"ttl\":%u}]"
        "}",
        fmt.string, (int) timestamp,
        port,
        name_from_ip_proto(ip_proto),
        status_string(status),
        reason_string(reason, reason_buffer, sizeof(reason_buffer)),
        ttl
    );

    // Produce
    kafka_produce(out, topic, buf, buflen);
}

/****************************************************************************
 ****************************************************************************/
static void
kafka_out_banner(struct Output *out, FILE *fp, time_t timestamp,
        ipaddress ip, unsigned ip_proto, unsigned port,
        enum ApplicationProtocol proto, unsigned ttl,
        const unsigned char *px, unsigned length)
{
    UNUSEDPARM(fp);

    char buf[1024]; // The json data
    int buflen;

    char banner_buffer[MAX_BANNER_LENGTH];
    char topic[32];

    ipaddress_formatted_t fmt = ipaddress_fmt(ip);

    // Build the kafka topic name
    snprintf(topic, sizeof(topic),
        "%s",
        masscan_app_to_string(proto)
    );

    // Build the json payload
    buflen = snprintf(buf, sizeof(buf),
        "{"
        "\"ip\":\"%s\",\"timestamp\": \"%d\","
        "\"ports\":[{\"port\":%u,\"proto\":\"%s\",\"ttl\":%u,"
        "\"service\":{\"name\":\"%s\",\"banner\":\"%s\"}}]"
        "}",
        fmt.string, (int) timestamp,
        port,
        name_from_ip_proto(ip_proto),
        ttl,
        masscan_app_to_string(proto),
        normalize_json_string(px, length, banner_buffer, sizeof(banner_buffer))
    );

    // Produce
    kafka_produce(out, topic, buf, buflen);
}


/****************************************************************************
 ****************************************************************************/
const struct OutputType kafka_output = {
    "kafka",
    0,
    kafka_out_open,
    kafka_out_close,
    kafka_out_status,
    kafka_out_banner
};

#endif
