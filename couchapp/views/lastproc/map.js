function(doc) {
    if (doc.status === 'good' && doc.conditions && doc.process) {
    	if(doc.process.length == 0)
        	emit([0, doc._id], doc.conditions);
        else if (doc.process[ doc.process.length-1 ].name) //emit the last process completed
        	emit( [doc.process[ doc.process.length-1 ].name, doc._id], doc.conditions);
    }
}