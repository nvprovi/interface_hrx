const https = require('https');
const http = require('http');
const parseString = require('xml2js').parseString;
const uuid = require('uuid');
var AWS = require('aws-sdk');
AWS.config.update({region: 'eu-central-1'});
var dynamoDB = new AWS.DynamoDB({apiVersion: '2012-08-10'});
const hafasIDs = require('./hafas_ids.json')



const buildRealTripElement = (siriJson) => {
  let zaehlfahrzeuge = [11,12,13,14,15,16,17,18,19,20,46,47,54,55,56,57];
  let isZaehlfahrzeug = zaehlfahrzeuge.includes(parseInt(siriJson['VehicleRef'])) ? true : false;
  let level = 1;
  if(siriJson['Occupancy']['OccupancyPercentage'] > 30){level = 2;}
  if(siriJson['Occupancy']['OccupancyPercentage'] > 60){level = 3;}
  // <UniqueID>` + unescape(encodeURIComponent(siriJson['DatedVehicleJourneyRef'])) + `</UniqueID> rausgenommen email Hoffmann 22.07.
  let xml = 
  `<RealTrip>
      <VehicleID>` + siriJson['VehicleRef'] + `</VehicleID> 
      <TripRef> 
          <TripID>
              <TripName>` + siriJson['LineRef'] + `</TripName>
          </TripID>
          <TripStartEnd>
              <StartStopID>` + unescape(encodeURIComponent(siriJson['OriginHAFAS'])) + `</StartStopID>
              <StartTime>` + siriJson['OriginAimedDepartureTime'] + `</StartTime>
              <EndStopID>` + unescape(encodeURIComponent(siriJson['DestinationHAFAS'])) + `</EndStopID>
              <EndTime>` + siriJson['DestinationAimedArrivalTime'] + `</EndTime>
          </TripStartEnd>
      </TripRef>
      <GeoPosition>
        <XCoordinate>` + siriJson['Location']['Longitude'] + `</XCoordinate> 
        <YCoordinate>` + siriJson['Location']['Latitude'] + `</YCoordinate> 
        <Timestamp>` + siriJson['RecordedAtTime'] + `</Timestamp>
      </GeoPosition>`
  if(isZaehlfahrzeug){
    xml += 
      `<Occupancy prognosis="false">          
          <Capacity>3</Capacity> 
          <Level>` + level + `</Level>
      </Occupancy>`;
  }
  xml +=`</RealTrip>`
  return xml;
}

exports.handler = async (event) => {
  const stopIDs = {};
  hafasIDs.forEach(function (element, index, array) {
    stopIDs[element.origin_id] = element.hafas_id;
  });
  console.log('size of stopIDs', Object.keys(stopIDs).length)
  
  let returnData = await Promise.all(event.Records.map(async (element) => {
    let buffer = new Buffer.from(element.kinesis.data, 'base64');
    let data = buffer.toString('ascii');
    let xmlData = await parseXml(data);
    let siriData = []
    xmlData.Siri.ServiceDelivery.forEach(element => {
      element.VehicleMonitoringDelivery.forEach(vehicleMonitoringData => {
        if(typeof(vehicleMonitoringData.VehicleActivity) === 'undefined'){return false;}
        vehicleMonitoringData.VehicleActivity.forEach(vehicleData => {
          // console.log(vehicleData)
          let occupancyArray = vehicleData.Extensions[0].OccupancyData[0]
          let locationArray = vehicleData.MonitoredVehicleJourney[0].VehicleLocation[0]
          let thisReturn = {
            'LineRef': false,
            'PublishedLineName': false,
            'DatedVehicleJourneyRef': false,
            'OriginRef': false,
            'DestinationRef': false,
            'OriginHAFAS': false,
            'DestinationHAFAS': false,
            'OriginAimedDepartureTime': false,
            'DestinationAimedArrivalTime': false,
            'DirectionRef': false,
            'VehicleRef': false,
            'RecordedAtTime': false,
            'Location': {},
            'Occupancy': {},
          }
          // vehicleData.MonitoredVehicleJourney[0].PreviousCalls[0].PreviousCall[0].StopPointRef[0]
          thisReturn['RecordedAtTime'] = vehicleData.RecordedAtTime[0]
          thisReturn['LineRef'] = vehicleData.MonitoredVehicleJourney[0].LineRef[0]
          thisReturn['PublishedLineName'] = vehicleData.MonitoredVehicleJourney[0].PublishedLineName[0]
          thisReturn['DatedVehicleJourneyRef'] = vehicleData.MonitoredVehicleJourney[0].FramedVehicleJourneyRef[0].DataFrameRef[0] + '-' + vehicleData.MonitoredVehicleJourney[0].FramedVehicleJourneyRef[0].DatedVehicleJourneyRef[0]
          
          thisReturn['OriginRef'] = vehicleData.MonitoredVehicleJourney[0].OriginRef[0]
          thisReturn['DestinationRef'] = vehicleData.MonitoredVehicleJourney[0].DestinationRef[0]
          thisReturn['OriginAimedDepartureTime'] = vehicleData.MonitoredVehicleJourney[0].OriginAimedDepartureTime[0]
          thisReturn['DestinationAimedArrivalTime'] = vehicleData.MonitoredVehicleJourney[0].DestinationAimedArrivalTime[0]
          thisReturn['DirectionRef'] = vehicleData.MonitoredVehicleJourney[0].DirectionRef[0]
          
          thisReturn['OriginHAFAS'] = getHafasStopID(thisReturn['OriginRef'], stopIDs);
          thisReturn['DestinationHAFAS'] = getHafasStopID(thisReturn['DestinationRef'], stopIDs);
          
          thisReturn['VehicleRef'] = vehicleData.MonitoredVehicleJourney[0].VehicleRef[0]
          thisReturn['Location']['Longitude'] = locationArray.Longitude[0]
          thisReturn['Location']['Latitude'] = locationArray.Latitude[0]
          thisReturn['Occupancy']['OccupancyPercentage'] = occupancyArray.OccupancyPercentage[0]
          thisReturn['Occupancy']['PassengersNumber'] = occupancyArray.PassengersNumber[0]
          thisReturn['Occupancy']['VehicleCapacity'] = occupancyArray.VehicleCapacity[0]
          thisReturn['Occupancy']['VehicleSeatsNumber'] = occupancyArray.VehicleSeatsNumber[0]
          /*
          thisReturn['PreviousCall']['StopPointRef'] = "x"
          thisReturn['OnwardCall']['StopPointRef'] = "y"
          thisReturn['MonitoredCall']['StopPointRef'] = vehicleData.MonitoredVehicleJourney[0].MonitoredCall[0].StopPointRef[0]
          thisReturn['MonitoredCall']['VisitNumber'] = vehicleData.MonitoredVehicleJourney[0].MonitoredCall[0].VisitNumber[0]
          thisReturn['MonitoredCall']['StopPointName'] = vehicleData.MonitoredVehicleJourney[0].MonitoredCall[0].StopPointName[0]
          thisReturn['MonitoredCall']['VehicleAtStop'] = vehicleData.MonitoredVehicleJourney[0].MonitoredCall[0].VehicleAtStop[0]
          if(typeof(vehicleData.MonitoredVehicleJourney[0].PreviousCalls) !== 'undefined'){
            thisReturn['PreviousCall']['StopPointRef'] = vehicleData.MonitoredVehicleJourney[0].PreviousCalls[0].PreviousCall[0].StopPointRef[0]
          }
          if(typeof(vehicleData.MonitoredVehicleJourney[0].OnwardCalls) !== 'undefined'){
            thisReturn['OnwardCall']['StopPointRef'] = vehicleData.MonitoredVehicleJourney[0].OnwardCalls[0].OnwardCall[0].StopPointRef[0]
          }
          */
          siriData.push(thisReturn);
        })
      })
    })
    console.log('size of data', siriData.length)
    return siriData
  }));
  
  let tripElements = []
  let recordedTime = []
  let returnXML = `<?xml version="1.0" encoding="UTF-8"?><ns1:Envelope xmlns:ns1="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ns2="urn:hrx" xmlns="urn:hrx"><ns1:Body><RealtimeInfo xmlns="urn:hrx" timestamp="2020-03-02T12:46:18.002+01:00" version="2.4.11" sender="Giessen">`;
  returnData.forEach(tripDataContainer => {
    tripDataContainer.forEach(tripDataObject => {
      //console.log(tripDataObject);
      recordedTime.push([tripDataObject['RecordedAtTime'], tripDataObject['LineRef'], tripDataObject['DatedVehicleJourneyRef']]);
      tripElements.push(tripDataObject);
      returnXML += buildRealTripElement(tripDataObject);
    })
  })      
  returnXML += "</RealtimeInfo></ns1:Body></ns1:Envelope>";
  var postDate = new Date();
  // console.log('returnXML ' + returnXML);
  let testdata = {
    'text': 'My Text',
    'time': postDate.toISOString()
  };
  //let responseXML2 = await doTestPost(JSON.stringify(testdata))
  let responseXML = await doPostRequest(returnXML)
  let responseXML_demo = await doPostRequestToDemo(returnXML)
  
  // console.log('response XML ' + responseXML);
  
  var item = {
    TableName: 'nvprovi_hacon_posts_production',
    Item: {
      'id' : {S: uuid.v4()},
      'post_time': {S: postDate.toISOString()},
      'post_json_summary' : {S: JSON.stringify(recordedTime)},
      'response_xml' : {S: responseXML.toString('utf8')}
    }
  };
  // 'post_xml' : {S: returnXML},
  // 'post_json_length': {S: 'number of items: ' + tripElements.length},
  await dynamoDB.putItem(item).promise();
  
  const response = {
      statusCode: 200,
      body: 'done',
  };
  return response;
};

/*
const getStopIDs = async (test) => {
  const params = {TableName: 'nvprovi_hafas_stop_ids'};
  return dynamoDB.scan(params, function (err, data) {
    if (err){console.log("get StopID error", err);} 
    else {console.log("get StopID success");} 
  }).promise();
};
*/

const getHafasStopID = (stop_ref, stopIDs) => {
  var index = (stop_ref.indexOf(' ') > -1) ? stop_ref.substr(0,stop_ref.indexOf(' ')).toUpperCase() : stop_ref;
  var returnVal = (stopIDs[index] !== 'undefined')  ? stopIDs[index] : "";
  return returnVal;
}

const parseXml = (xml) => {
  return new Promise((resolve, reject) => {
    parseString(xml, (err, result) => {
      if (err) {
        reject(err);
      } else {
        resolve(result);
      }
    });
  });
}

const doPostRequest = async (data) => {
  return new Promise((resolve, reject) => {
    const options = {
      host: '',
      port: 0,
      path: '',
      method: 'POST'
    };
    let responseBody = "";
    //create the request object with the callback with the result
    const req = http.request(options, (res) => {
      console.log('hacon prod response status', res.statusCode);
      console.log('hacon prod response message', res.statusMessage);
      res.on('data', function (chunk) {
        responseBody = chunk;
      });
      res.on('end', (e) => {
        resolve(responseBody);
      });
    });

    req.on('error', (e) => {
      console.log('error', e.message);
      reject(e.message);
    });
    req.write(data);
    req.end();
  });
};


const doPostRequestToDemo = async (data) => {
  return new Promise((resolve, reject) => {
    const options = {
      host: '',
      path: '',
      method: 'POST'
    };
    let responseBody = "";
    //create the request object with the callback with the result
    const req = https.request(options, (res) => {
      console.log('hacon demo response status', res.statusCode);
      console.log('hacon demo response message', res.statusMessage);
      res.on('data', function (chunk) {
        responseBody = chunk;
      });
      res.on('end', (e) => {
        resolve(responseBody);
      });
    });

    req.on('error', (e) => {
      console.log('error', e.message);
      reject(e.message);
    });
    req.write(data);
    req.end();
  });
};

const doTestPost = async (data) => {
  return new Promise((resolve, reject) => {
    const options = {
      host: '',
      path: '',
      method: 'POST'
    };
    let responseBody = "";
    //create the request object with the callback with the result
    const req = https.request(options, (res) => {
      console.log('test response status', res.statusCode);
      console.log('test response message', res.statusMessage);
      res.on('data', function (chunk) {
        responseBody = chunk;
      });
      res.on('end', (e) => {
        resolve(responseBody);
      });
    });

    req.on('error', (e) => {
      console.log('error', e.message);
      reject(e.message);
    });
    req.write(data);
    req.end();
  });
};