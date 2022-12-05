#include <sys/types.h>
// #include <sys/eventfd.h>
#include <unistd.h>
#include <caml/memory.h>

// CAMLprim value caml_eio_unix_eventfd(value v_initval) {
//   int ret;
//   ret = eventfd(Int_val(v_initval), EFD_CLOEXEC);
//   if (ret == -1) uerror("eventfd", Nothing);
//   return Val_int(ret);
// }