package main

import (
	"flag"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/credentials"
	"log"
	pb "eventhub/predix"
	"google.golang.org/grpc/testdata"
)

var (
	tls                = flag.Bool("tls", false, "Connection uses TLS if true, else plain TCP")
	caFile             = flag.String("ca_file", "", "The file containning the CA root cert file")
	serverAddr         = flag.String("server_addr", "event-hub-aws-usw02.data-services.predix.io:443", "The server address in the format of host:port")
	serverHostOverride = flag.String("server_host_override", "event-hub-aws-usw02.data-services.predix.io", "The server name use to verify the hostname returned by TLS handshake")
)

func main() {
	flag.Parse()
	var opts []grpc.DialOption
	if *tls {
		if *caFile == "" {
			*caFile = testdata.Path("ca.pem")
		}
		creds, err := credentials.NewClientTLSFromFile(*caFile, *serverHostOverride)
		if err != nil {
			log.Fatalf("Failed to create TLS credentials %v", err)
		}
		opts = append(opts, grpc.WithTransportCredentials(creds))
	} else {
		opts = append(opts, grpc.WithInsecure())
	}
	header := metadata.New(map[string]string{"authorization": "eyJhbGciOiJSUzI1NiIsImtpZCI6Im5JakxxIiwidHlwIjoiSldUIn0.eyJqdGkiOiI3MTFlZDEwYmUyMjQ0MzQ4YmFjM2Y4MzQ1ZGI5YThiMSIsInN1YiI6ImV2ZW50X2h1Yl90ZXN0Iiwic2NvcGUiOlsidWFhLm5vbmUiLCJwcmVkaXgtZXZlbnQtaHViLnpvbmVzLjFkZjM0MDQ3LWVjZTAtNDUyNy05ZDhmLWUyNmEwMzk5NmRkYi51c2VyIiwicHJlZGl4LWV2ZW50LWh1Yi56b25lcy4xZGYzNDA0Ny1lY2UwLTQ1MjctOWQ4Zi1lMjZhMDM5OTZkZGIuZ3JwYy5wdWJsaXNoIiwicHJlZGl4LWV2ZW50LWh1Yi56b25lcy4xZGYzNDA0Ny1lY2UwLTQ1MjctOWQ4Zi1lMjZhMDM5OTZkZGIud3NzLnB1Ymxpc2giXSwiY2xpZW50X2lkIjoiZXZlbnRfaHViX3Rlc3QiLCJjaWQiOiJldmVudF9odWJfdGVzdCIsImF6cCI6ImV2ZW50X2h1Yl90ZXN0IiwiZ3JhbnRfdHlwZSI6ImNsaWVudF9jcmVkZW50aWFscyIsInJldl9zaWciOiJlYzVjNzBiNyIsImlhdCI6MTUwMjUxMzk3NSwiZXhwIjoxNTAyNTU3MTc1LCJpc3MiOiJodHRwczovLzM2NGZhZGY0LTk5ZDItNDM5Ny05NGE4LTA3NzFjMWJlODkxOC5wcmVkaXgtdWFhLnJ1bi5hd3MtdXN3MDItcHIuaWNlLnByZWRpeC5pby9vYXV0aC90b2tlbiIsInppZCI6IjM2NGZhZGY0LTk5ZDItNDM5Ny05NGE4LTA3NzFjMWJlODkxOCIsImF1ZCI6WyJwcmVkaXgtZXZlbnQtaHViLnpvbmVzLjFkZjM0MDQ3LWVjZTAtNDUyNy05ZDhmLWUyNmEwMzk5NmRkYi5ncnBjIiwicHJlZGl4LWV2ZW50LWh1Yi56b25lcy4xZGYzNDA0Ny1lY2UwLTQ1MjctOWQ4Zi1lMjZhMDM5OTZkZGIiLCJwcmVkaXgtZXZlbnQtaHViLnpvbmVzLjFkZjM0MDQ3LWVjZTAtNDUyNy05ZDhmLWUyNmEwMzk5NmRkYi53c3MiLCJldmVudF9odWJfdGVzdCJdfQ.E58J9pbaCAKXXkpm4WltrMRy5GGD3HD8MKJBFdAftiavS-Cxvo9GC1NOauWnL3LZcUtexk7QREd1sn00IVyERnPar_U53XpltNBZfK-bQtmPIRMX0-Dp3nUbROrnqVu2VmhAkSAxTggukiJ-dm8a5vGsGOFPAOryFQcCjsvQLBlNMOAfz50XHDsuMsrnodiqd6okhIcFti66yudpEsoj_tuSdOIURTKBEzCE1baY-4Tt8rwUtKBSXN6WkxX6XyPSLSuDBY7bSFew3jpYrvKdb-nbWaCyzbIq-c1hAeoM5Kt619P2mLy9wBo83dv47JlasQYXorEnnGNpR3q1ZnV0Dw"})
	conn, err := grpc.Dial(*serverAddr, opts...)
	defer conn.Close()
	ctx := metadata.NewContext(context.Background(), header)
	cancelCtx, _ := context.WithCancel(ctx)
	md, _ := metadata.FromContext(cancelCtx)
	client := pb.NewPublisherClient(conn)
	stream, err := client.Send(ctx)
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	// Compose messages
	var messages []*pb.Message
	var tags map[string]string
	tags = make(map[string]string)
	tags["my_tag"] = "my_tag"
	messages = append(messages, &pb.Message{
		Id: "001",
		Body: []byte(`{
			"attribute1": "value1",
			"attribute2": "value2",
		}`),
		ZoneId: md["predix-zone-id"][0],
		Tags:   tags,
	})

	// Send messages to Event Hub
	stream.Send(&pb.PublishRequest{
		Messages: &pb.Messages{
			Msg: messages,
		},
	})
}