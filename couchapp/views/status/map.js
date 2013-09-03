function(doc){
  if(doc.status){
    emit(doc.status, 1);
  }
}