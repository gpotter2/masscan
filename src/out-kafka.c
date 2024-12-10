#include "output.h"
#include "masscan.h"
#include "pixie-sockets.h"
#include "util-logger.h"
#include <ctype.h>

int kafka_produce(
        char* buf,
        char* topic_name,
        int topic_len,
        unsigned long timestamp,
        char* payload,
        int length)
{
    const char client_id[] = "masscan";
    int client_len = sizeof(client_id) - 1;
    int i = 0;

    // Build packet
    i += 4;  // Skip initial length

    buf[i + 1] = 0;  // API Key: Produce
    i += 2;

    buf[i + 1] = 2;  // API version
    i += 2;

    buf[i + 3] = 1;  // Correlation ID
    i += 4;

    buf[i]     = client_len >> 8;  // Client ID
    buf[i + 1] = client_len & 0xff;
    i += 2;
    memcpy(&buf[i], client_id, client_len);
    i += client_len;

    buf[i + 1] = 1;  // Required acks
    i += 2;

    buf[i + 2] = 0x5;  // Timeout
    buf[i + 3] = 0xdc;
    i += 4;

    buf[i + 3] = 1;  // N° of topics
    i += 4;

    buf[i]     = topic_len >> 8;  // Topic name
    buf[i + 1] = topic_len & 0xff;
    i += 2;
    memcpy(&buf[i], topic_name, topic_len);
    i += topic_len;

    buf[i + 3] = 1;  // N° of partitions
    i += 4;

    buf[i + 3] = 0;  // Partition ID
    i += 4;

    buf[i]     = ((length + 0x22) >> 24) & 0xFF;  // Message set size
    buf[i + 1] = ((length + 0x22) >> 16) & 0xFF;
    buf[i + 2] = ((length + 0x22) >> 8) & 0xFF;
    buf[i + 3] = (length + 0x22) & 0xFF;
    i += 4;

    i += 8;  // Offset = 0

    buf[i]     = ((length + 0x16) >> 24) & 0xFF;  // Message size
    buf[i + 1] = ((length + 0x16) >> 16) & 0xFF;
    buf[i + 2] = ((length + 0x16) >> 8) & 0xFF;
    buf[i + 3] = (length + 0x16) & 0xFF;
    i += 4;

    i += 4; // CRC32

    buf[i] = 1;  // Magic Byte
    i += 1;

    buf[i] = 0;  // Compression
    i += 1;

    buf[i]     = (timestamp >> 56) & 0xFF;  // Timestamp
    buf[i + 1] = (timestamp >> 48) & 0xFF;
    buf[i + 2] = (timestamp >> 40) & 0xFF;
    buf[i + 3] = (timestamp >> 32) & 0xFF;
    buf[i + 4] = (timestamp >> 24) & 0xFF;
    buf[i + 5] = (timestamp >> 16) & 0xFF;
    buf[i + 6] = (timestamp >> 8) & 0xFF;
    buf[i + 7] = timestamp & 0xFF;
    i += 8;

    buf[i]     = 0xff;  // Key = Missing
    buf[i + 1] = 0xff;
    buf[i + 2] = 0xff;
    buf[i + 3] = 0xff;
    i += 4;

    buf[i]     = (length >> 24) & 0xFF;  // Value size
    buf[i + 1] = (length >> 16) & 0xFF;
    buf[i + 2] = (length >> 8) & 0xFF;
    buf[i + 3] = length & 0xFF;
    i += 4;

    memcpy(&buf[i], payload, length);  // Value
    i += length;

    buf[0] = ((i - 4) >> 24) & 0xFF;  // Total length
    buf[1] = ((i - 4) >> 16) & 0xFF;
    buf[2] = ((i - 4) >> 8) & 0xFF;
    buf[3] =  (i - 4) & 0xFF;

    return i;
}

/****************************************************************************
 ****************************************************************************/
static void
kafka_out_open(struct Output *out, FILE *fp)
{
    // To remove the , in JSON. Honnestly just look at out-json.c 
    out->is_first_record_seen = 1;
}

/****************************************************************************
 ****************************************************************************/
static void
kafka_out_close(struct Output *out, FILE *fp)
{
}

/****************************************************************************
 ****************************************************************************/
static void
kafka_out_status(struct Output *out, FILE *fp, time_t timestamp,
    int status, ipaddress ip, unsigned ip_proto, unsigned port, unsigned reason, unsigned ttl)
{
    UNUSEDPARM(out);
    ptrdiff_t fd = (ptrdiff_t)fp;

    int count;
    char data[1024]; // The json data
    char buf[1024]; // The kafka packet
    int length;
    
    char topic_name[32];
    int topic_name_length;
    char reason_buffer[128];

    ipaddress_formatted_t fmt = ipaddress_fmt(ip);
   
    // Build the kafka topic name 
    topic_name_length = snprintf(topic_name, sizeof(topic_name),
        "%s-%u",
        name_from_ip_proto(ip_proto),
        port
    );

    // Build the json payload
    length = snprintf(data, sizeof(data),
        "{"
        "\"ip\":\"%s\","
        "\"ports\":[{\"port\":%u,\"proto\":\"%s\",\"status\":\"%d\","
        "\"reason\":\"%s\",\"ttl\":%u}]"
        "}",
        fmt.string,
        port,
        name_from_ip_proto(ip_proto),
        status,
        reason_string(reason, reason_buffer, sizeof(reason_buffer)),
        ttl
    );
  
    // Build the kafka produce payload 
    memset(buf, 0, sizeof(buf)); 
    length = kafka_produce(buf, topic_name, topic_name_length,
                           (unsigned long) timestamp, data, length);
    
    count = send((SOCKET)fd, buf, length, 0);
    if (count != length) {
        LOG(0, "redis: error sending data\n");
        exit(1);
    }
}

/****************************************************************************
 ****************************************************************************/
static void
kafka_out_banner(struct Output *out, FILE *fp, time_t timestamp,
        ipaddress ip, unsigned ip_proto, unsigned port,
        enum ApplicationProtocol proto, unsigned ttl,
        const unsigned char *px, unsigned length)
{
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
