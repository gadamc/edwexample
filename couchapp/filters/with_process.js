function(doc, req){

  if(!doc.process)
    return false;

  if (req.query.last) {
      
    if(doc.process.length == 0)
      return req.query.last === '0';
        
    return doc.process[ doc.process.length-1 ].name === req.query.last;
  }

  //could add other query parameters, such as 'first' or size to match against the number of values in the doc.process array

  return false;
}